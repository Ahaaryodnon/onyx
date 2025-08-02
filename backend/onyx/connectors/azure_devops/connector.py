from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Iterator, List

from azure.devops.connection import Connection
from azure.devops.v7_1.work_item_tracking.models import Wiql
from azure.devops.v7_1.work_item_tracking.work_item_tracking_client import WorkItemTrackingClient
from msrest.authentication import BasicAuthentication

from onyx.configs.app_configs import INDEX_BATCH_SIZE
from onyx.configs.constants import DocumentSource
from onyx.connectors.interfaces import (
    GenerateDocumentsOutput,
    LoadConnector,
    PollConnector,
    SecondsSinceUnixEpoch,
)
from onyx.connectors.models import BasicExpertInfo, ConnectorMissingCredentialError, Document, TextSection
from onyx.utils.logger import setup_logger

logger = setup_logger()


class AzureDevopsConnector(LoadConnector, PollConnector):
    def __init__(self, organization: str, project: str, batch_size: int = INDEX_BATCH_SIZE) -> None:
        self.organization = organization
        self.project = project
        self.batch_size = batch_size
        self.client: WorkItemTrackingClient | None = None

    def load_credentials(self, credentials: dict[str, Any]) -> dict[str, Any] | None:
        pat = credentials.get("azure_devops_pat")
        if not isinstance(pat, str):
            raise ConnectorMissingCredentialError("Azure DevOps PAT missing or invalid")
        connection = Connection(
            base_url=f"https://dev.azure.com/{self.organization}",
            creds=BasicAuthentication("", pat),
        )
        self.client = connection.clients.get_work_item_tracking_client()
        return None

    def _fetch_work_items(
        self,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> GenerateDocumentsOutput:
        if not self.client:
            raise ConnectorMissingCredentialError("Azure DevOps")
        query = f"SELECT [System.Id] FROM WorkItems WHERE [System.TeamProject]='{self.project}'"
        if start:
            query += f" AND [System.ChangedDate] >= '{start.isoformat()}'"
        if end:
            query += f" AND [System.ChangedDate] <= '{end.isoformat()}'"
        query += " ORDER BY [System.ChangedDate] ASC"
        result = self.client.query_by_wiql(Wiql(query=query))
        ids = [wi.id for wi in result.work_items] if result.work_items else []
        for i in range(0, len(ids), self.batch_size):
            batch_ids = ids[i : i + self.batch_size]
            items = self.client.get_work_items(batch_ids, project=self.project, expand="Fields")
            docs: List[Document] = []
            for item in items:
                fields = item.fields or {}
                updated_at = fields.get("System.ChangedDate")
                if isinstance(updated_at, str):
                    try:
                        updated_at_dt = datetime.fromisoformat(updated_at.rstrip("Z"))
                    except Exception:
                        updated_at_dt = datetime.utcnow()
                elif isinstance(updated_at, datetime):
                    updated_at_dt = updated_at
                else:
                    updated_at_dt = datetime.utcnow()
                updated_at_dt = updated_at_dt.replace(tzinfo=timezone.utc)
                doc = Document(
                    id=str(item.id),
                    sections=[TextSection(link=item.url, text=fields.get("System.Description", ""))],
                    source=DocumentSource.AZURE_DEVOPS,
                    semantic_identifier=fields.get("System.Title", f"Work Item {item.id}"),
                    doc_updated_at=updated_at_dt,
                    primary_owners=[
                        BasicExpertInfo(display_name=fields.get("System.AssignedTo"))
                    ]
                    if fields.get("System.AssignedTo")
                    else [],
                    metadata={"state": fields.get("System.State")},
                )
                docs.append(doc)
            if docs:
                yield docs

    def load_from_state(self) -> GenerateDocumentsOutput:
        return self._fetch_work_items()

    def poll_source(self, start: SecondsSinceUnixEpoch, end: SecondsSinceUnixEpoch) -> GenerateDocumentsOutput:
        start_dt = datetime.fromtimestamp(start, tz=timezone.utc)
        end_dt = datetime.fromtimestamp(end, tz=timezone.utc)
        return self._fetch_work_items(start_dt, end_dt)
