from datetime import timedelta
import json
import logging
import time

from src.core.use_cases.disable_lambda_triggers import DisableLambdaTriggers

from src.infrastructure.adapters.lambda_serverless_function import (
    LambdaServerlessFunction,
)

from src.logging_config import setup_logging

from src.infrastructure.constants import (
    ORCHESTRATOR_FUNCTION_NAME,
    WORKER_FUNCTION_NAME,
)

setup_logging()
logger = logging.getLogger(__name__)

# adapters
serverless_function = LambdaServerlessFunction()

# use cases
disable_triggers_job = DisableLambdaTriggers(serverless_function=serverless_function)


TARGET_FUNCTIONS = [ORCHESTRATOR_FUNCTION_NAME, WORKER_FUNCTION_NAME]


def lambda_handler(event, context):
    start_time = time.monotonic()

    for record in event.get("Records", []):
        try:
            logger.info("Starting Lambda Trigger Management Job...")

            sns_message_string = record.get("Sns", {}).get("Message", "{}")
            cloudwatch_payload = json.loads(sns_message_string)

            alarm_name = cloudwatch_payload.get("AlarmName", "Unknown")
            new_state = cloudwatch_payload.get("NewStateValue", "")

            logger.info(
                f"Triggered by CloudWatch Alarm: '{alarm_name}'. State: '{new_state}'"
            )

            if new_state != "ALARM":
                logger.info("Alarm state is not ALARM. Ignored.")
                continue

            for function_name in TARGET_FUNCTIONS:
                disable_triggers_job.run(function_name)

            logger.info("Lambda Trigger Management Job completed successfully.")

        except Exception as e:
            logger.error(f"Error processing record: {e}", exc_info=True)

    end_time = time.monotonic()
    elapsed_time = timedelta(seconds=end_time - start_time)
    logger.info(f"Total elapsed time: {elapsed_time}")
