import logging
from src.core.interfaces.serverless_function import ServerlessFunctionProvider

logger = logging.getLogger(__name__)


class EnableLambdaTriggers:
    def __init__(self, serverless_function: ServerlessFunctionProvider):
        self.serverless_function = serverless_function

    def run(self, function_name: str):
        try:
            self.serverless_function.enable_triggers(function_name)
            logger.info(f"Successfully enabled triggers for {function_name}")

        except Exception as e:
            logger.error(f"Error enabling triggers for {function_name}: {e}")
