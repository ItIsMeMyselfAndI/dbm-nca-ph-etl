import logging
from src.core.interfaces.serverless_function import ServerlessFunctionProvider

logger = logging.getLogger(__name__)


class DisableLambdaTriggers:
    def __init__(self, serverless_function: ServerlessFunctionProvider):
        self.serverless_function = serverless_function

    def run(self, function_name: str):
        try:
            self.serverless_function.disable_triggers(function_name)
            logger.info(f"Successfully disabled triggers for {function_name}")

        except Exception as e:
            logger.error(f"Error disabling triggers for {function_name}: {e}")
