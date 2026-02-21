from typing import List

import boto3

from src.core.interfaces.serverless_function import ServerlessFunctionProvider


class LambdaServerlessFunction(ServerlessFunctionProvider):
    def __init__(self):
        self.lambda_client = boto3.client("lambda")

    def enable_triggers(self, function_name: str) -> None:
        trigger_uuids = self._get_trigger_uuids(function_name)
        for uuid in trigger_uuids:
            self.lambda_client.update_event_source_mapping(UUID=uuid, Enabled=True)

    def disable_triggers(self, function_name: str) -> None:
        trigger_uuids = self._get_trigger_uuids(function_name)
        for uuid in trigger_uuids:
            self.lambda_client.update_event_source_mapping(UUID=uuid, Enabled=False)

    def _get_trigger_uuids(self, function_name: str) -> List[str]:
        response = self.lambda_client.list_event_source_mappings(
            FunctionName=function_name
        )
        trigger_uuids = []
        for mapping in response.get("EventSourceMappings", []):
            trigger_uuids.append(mapping["UUID"])
        return trigger_uuids
