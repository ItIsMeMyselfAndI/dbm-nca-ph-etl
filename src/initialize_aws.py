from io import BytesIO
import json
import logging
import zipfile
import boto3
from botocore.exceptions import ClientError

from src.infrastructure.config import settings
from src.logging_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)

ENV_VARS = {
    "SUPABASE_URL": settings.SUPABASE_URL,
    "SUPABASE_ANON_KEY": settings.SUPABASE_ANON_KEY,
    "AWS_S3_BUCKET_NAME": settings.AWS_S3_BUCKET_NAME,
    "AWS_SQS_RELEASE_QUEUE_URL": settings.AWS_SQS_RELEASE_QUEUE_URL,
    "AWS_SQS_RELEASE_BATCH_QUEUE_URL": settings.AWS_SQS_RELEASE_BATCH_QUEUE_URL,
}


def main():
    release_files_bucket_name = "dbm-nca-ph-release-files"
    lambda_deployments_bucket_name = "dbm-nca-ph-lambda-deployments"

    dlq_name = "dbm-nca-ph-failed-queues"
    release_queue_name = "dbm-nca-ph-release-queue"
    release_batch_queue_name = "dbm-nca-ph-release-batch-queue"

    lambda_role_name = "dbm-nca-ph-lambda-role"

    scraper_lambda_name = "dbmScraper"
    orchestrator_lambda_name = "dbmOrchestrator"
    worker_lambda_name = "dbmWorker"

    # buckets
    if not is_bucket_exists(release_files_bucket_name):
        create_bucket(release_batch_queue_name)

    if not is_bucket_exists(lambda_deployments_bucket_name):
        create_bucket(lambda_deployments_bucket_name)

    # dead letter queue
    dlq_info = get_queue(dlq_name)
    if not dlq_info:
        dlq_info = create_queue(dlq_name)

    # queues
    release_queue_info = get_queue(release_queue_name)
    if not release_queue_info and dlq_info:
        release_queue_info = create_queue(release_queue_name, dlq_info["arn"])

    release_batch_queue_info = get_queue(release_batch_queue_name)
    if not release_batch_queue_info and dlq_info:
        release_batch_queue_info = create_queue(
            release_batch_queue_name, dlq_info["arn"]
        )

    if not release_queue_info or not release_batch_queue_info:
        logger.error("Failed to create necessary SQS queues.")
        return

    # iam role
    lambda_role_info = get_iam_role(lambda_role_name)
    if not lambda_role_info:
        lambda_role_info = create_lambda_iam_role(lambda_role_name)

    if not lambda_role_info:
        logger.error("Failed to create necessary IAM role for Lambda functions.")
        return

    # lambdas
    scraper_lambda_info = get_lambda_function(scraper_lambda_name)
    if not scraper_lambda_info:
        scraper_lambda_info = create_lambda_function(
            function_name=scraper_lambda_name,
            role_arn=lambda_role_info["arn"],
        )

    orchestrator_lambda_info = get_lambda_function(orchestrator_lambda_name)
    if not orchestrator_lambda_info:
        orchestrator_lambda_info = create_lambda_function(
            function_name=orchestrator_lambda_name,
            role_arn=lambda_role_info["arn"],
            queue_arn=release_queue_info.get("arn", None),
        )

    worker_lambda_info = get_lambda_function(worker_lambda_name)
    if not worker_lambda_info and lambda_role_info and release_queue_info:
        worker_lambda_info = create_lambda_function(
            function_name=worker_lambda_name,
            role_arn=lambda_role_info["arn"],
            queue_arn=release_batch_queue_info.get("arn", None),
        )


def is_bucket_exists(bucket_name: str) -> bool:
    s3 = boto3.client("s3")
    try:
        s3.head_bucket(Bucket=bucket_name)
        logger.info(f"Bucket '{bucket_name}' exists.")
        return True

    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            logger.info(f"Bucket '{bucket_name}' does not exist.")
            return False
        else:
            logger.error(f"Error checking bucket existence: {e}")
        return False


def create_bucket(bucket_name: str) -> None:
    s3 = boto3.client("s3")
    try:
        s3.create_bucket(Bucket=bucket_name)
        logger.info(f"Bucket '{bucket_name}' created successfully.")

    except Exception as e:
        logger.error(f"Error creating bucket: {e}")


def get_queue(queue_name: str, quiet: bool = False) -> dict | None:
    sqs = boto3.client("sqs")
    try:
        queue_url = sqs.get_queue_url(QueueName=queue_name).get("QueueUrl", None)
        if not queue_url:
            logger.info(f"Queue '{queue_name}' does not exist.")
            return None
        response = sqs.get_queue_attributes(
            QueueUrl=queue_url, AttributeNames=["QueueArn"]
        )
        if not quiet:
            logger.info(
                f"Queue '{queue_name}' exists"
                f"\n(URL) {queue_url}"
                f"\n(ARN) {response['Attributes']['QueueArn']}"
            )
        return {"url": queue_url, "arn": response["Attributes"]["QueueArn"]}

    except ClientError as e:
        if e.response["Error"]["Code"] == "AWS.SimpleQueueService.NonExistentQueue":
            logger.info(f"Queue '{queue_name}' does not exist.")
            return None
        else:
            logger.error(f"Error getting queue: {e}")
            return None


def create_queue(queue_name: str, dlq_arn: str | None = None) -> dict | None:
    sqs = boto3.client("sqs")
    attributes = {
        "VisibilityTimeout": "600",
        "MessageRetentionPeriod": "86400",
        "DelaySeconds": "0",
        "ReceiveMessageWaitTimeSeconds": "20",
    }
    if dlq_arn:
        redrive_policy = {"maxReceiveCount": "1", "deadLetterTargetArn": dlq_arn}
        attributes["RedrivePolicy"] = json.dumps(redrive_policy)

    try:
        sqs.create_queue(QueueName=queue_name, Attributes=attributes)

        response = get_queue(queue_name)
        if not response:
            logger.error(f"Failed to retrieve queue '{queue_name}' after creation.")
            return None
        logger.info(
            f"Queue '{queue_name}' created successfully"
            f"\n(URL) {response['url']}"
            f"\n(ARN) {response['arn']}"
        )

        return {"url": response["url"], "arn": response["arn"]}

    except Exception as e:
        logger.error(f"Error creating queue '{queue_name}': {e}")
        return None


def get_iam_role(role_name: str) -> dict | None:
    iam_client = boto3.client("iam")
    try:
        response = iam_client.get_role(RoleName=role_name)
        logger.info(
            f"IAM role '{role_name}' exists" f"\n(ARN) {response['Role']['Arn']}"
        )
        return {"arn": response["Role"]["Arn"]}

    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchEntity":
            logger.info(f"IAM role '{role_name}' does not exist.")
            return None
        else:
            logger.error(f"Error getting IAM role: {e}")
            return None


def create_lambda_iam_role(role_name: str) -> dict | None:
    iam_client = boto3.client("iam")
    assume_role_policy_document = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": {"Service": "lambda.amazonaws.com"},
                "Action": "sts:AssumeRole",
            }
        ],
    }
    policy_arns = [
        "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole",
        "arn:aws:iam::aws:policy/service-role/AWSLambdaSQSQueueExecutionRole",
        "arn:aws:iam::aws:policy/AmazonSQSFullAccess",
        "arn:aws:iam::aws:policy/AmazonS3FullAccess",
    ]

    try:
        response = iam_client.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=json.dumps(assume_role_policy_document),
        )
        for policy_arn in policy_arns:
            iam_client.attach_role_policy(RoleName=role_name, PolicyArn=policy_arn)
        logger.info(
            f"IAM role '{role_name}' created successfully"
            f"\n(ARN) {response['Role']['Arn']}"
        )
        return {"arn": response["Role"]["Arn"]}

    except Exception as e:
        logger.error(f"Error creating IAM role: {e}")
        return None


def get_lambda_function(function_name: str) -> dict | None:
    lambda_client = boto3.client("lambda")
    try:
        response = lambda_client.get_function(FunctionName=function_name)
        logger.info(
            f"Lambda function '{function_name}' exists"
            f"\n(ARN) {response['Configuration']['FunctionArn']}"
        )
        return {"arn": response["Configuration"]["FunctionArn"]}

    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceNotFoundException":
            logger.info(f"Lambda function '{function_name}' does not exist.")
            return None
        else:
            logger.error(f"Error getting Lambda function: {e}")
            return None


def create_lambda_function(
    function_name: str,
    role_arn: str,
    queue_arn: str | None = None,
    queue_batch_size: int = 10,
) -> dict | None:
    lambda_client = boto3.client("lambda")

    # inital code
    zip_buffer = BytesIO()
    with zipfile.ZipFile(zip_buffer, "a", zipfile.ZIP_DEFLATED) as zf:
        default_code = "def lambda_handler(event, context):\n    return {'statusCode': 200, 'body': 'Hello from Lambda!'}"
        zf.writestr("lambda_function.py", default_code)
    zip_buffer.seek(0)
    zipped_code = zip_buffer.read()

    try:
        response = lambda_client.create_function(
            FunctionName=function_name,
            Runtime="python3.14",
            Role=role_arn,
            Timeout=300,
            MemorySize=512,
            Code={"ZipFile": zipped_code},
            Handler="lambda_function.lambda_handler",
            Environment={"Variables": ENV_VARS},
        )

        if queue_arn:
            lambda_client.create_event_source_mapping(
                EventSourceArn=queue_arn,
                FunctionName=function_name,
                BatchSize=queue_batch_size,
                Enabled=True,
            )
        logger.info(
            f"Lambda function '{function_name}' created successfully"
            f"\n(ARN) {response['FunctionArn']}"
        )
        return {"arn": response["FunctionArn"]}

    except Exception as e:
        logger.error(f"Error creating Lambda function: {e}")
        return None


if __name__ == "__main__":
    main()
