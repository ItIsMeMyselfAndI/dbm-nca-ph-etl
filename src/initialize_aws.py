from io import BytesIO
import json
import logging
import zipfile
import boto3
from botocore.exceptions import ClientError

from src.infrastructure.config import settings
from src.infrastructure.constants import (
    DLQ_NAME,
    ORCHESTRATOR_FUNCTION_NAME,
    RELEASE_BATCH_ALARM_NAME,
    RELEASE_BATCH_QUEUE_NAME,
    RELEASE_BATCH_SNS_TOPIC_NAME,
    RELEASE_QUEUE_NAME,
    SCRAPER_FUNCTION_NAME,
    TEARDOWN_FUNCTION_NAME,
    WORKER_FUNCTION_NAME,
)
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

    lambda_role_name = "dbm-nca-ph-lambda-role"

    # buckets
    if not is_bucket_exists(release_files_bucket_name):
        create_bucket(release_files_bucket_name)

    if not is_bucket_exists(lambda_deployments_bucket_name):
        create_bucket(lambda_deployments_bucket_name)

    # dead letter queue
    dlq_info = get_queue(DLQ_NAME)
    if not dlq_info:
        dlq_info = create_queue(DLQ_NAME)

    # queues
    release_queue_info = get_queue(RELEASE_QUEUE_NAME)
    if not release_queue_info and dlq_info:
        release_queue_info = create_queue(RELEASE_QUEUE_NAME, dlq_info["arn"])

    release_batch_queue_info = get_queue(RELEASE_BATCH_QUEUE_NAME)
    if not release_batch_queue_info and dlq_info:
        release_batch_queue_info = create_queue(
            RELEASE_BATCH_QUEUE_NAME, dlq_info["arn"]
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
    scraper_lambda_info = get_lambda_function(SCRAPER_FUNCTION_NAME)
    if not scraper_lambda_info:
        scraper_lambda_info = create_lambda_function(
            function_name=SCRAPER_FUNCTION_NAME,
            role_arn=lambda_role_info["arn"],
            max_cuncurrent_executions=40,
        )

    orchestrator_lambda_info = get_lambda_function(ORCHESTRATOR_FUNCTION_NAME)
    if not orchestrator_lambda_info:
        orchestrator_lambda_info = create_lambda_function(
            function_name=ORCHESTRATOR_FUNCTION_NAME,
            role_arn=lambda_role_info["arn"],
            queue_arn=release_queue_info.get("arn", None),
            queue_batch_size=10,
            max_cuncurrent_executions=40,
        )

    worker_lambda_info = get_lambda_function(WORKER_FUNCTION_NAME)
    if not worker_lambda_info and lambda_role_info and release_queue_info:
        worker_lambda_info = create_lambda_function(
            function_name=WORKER_FUNCTION_NAME,
            role_arn=lambda_role_info["arn"],
            queue_arn=release_batch_queue_info.get("arn", None),
            queue_batch_size=1,
            max_cuncurrent_executions=40,
        )

    teardown_lambda_info = get_lambda_function(TEARDOWN_FUNCTION_NAME)
    if not teardown_lambda_info:
        teardown_lambda_info = create_lambda_function(
            function_name=TEARDOWN_FUNCTION_NAME,
            role_arn=lambda_role_info["arn"],
            max_cuncurrent_executions=1,
        )

    # notification
    release_batch_sns_topic_info = get_sns_topic(RELEASE_BATCH_SNS_TOPIC_NAME)
    if not release_batch_sns_topic_info:
        release_batch_sns_topic_info = create_sns_topic(RELEASE_BATCH_SNS_TOPIC_NAME)

    # subscription
    if release_batch_sns_topic_info and teardown_lambda_info:
        release_batch_subscription_info = get_sns_lambda_subscription(
            topic_arn=release_batch_sns_topic_info["arn"],
            lambda_arn=teardown_lambda_info["arn"],
        )
        if not release_batch_subscription_info:
            subscribe_lambda_to_sns_topic(
                topic_arn=release_batch_sns_topic_info["arn"],
                lambda_arn=teardown_lambda_info["arn"],
            )

    if release_batch_sns_topic_info:
        add_lambda_sns_permission(
            TEARDOWN_FUNCTION_NAME, release_batch_sns_topic_info["arn"]
        )

    # cloudwatch
    release_batch_cloudwatch_alarm_info = get_cloudwatch_alarm(RELEASE_BATCH_ALARM_NAME)
    if (
        release_batch_sns_topic_info
        and release_batch_queue_info
        and not release_batch_cloudwatch_alarm_info
    ):
        create_cloudwatch_alarm(
            alarm_name=RELEASE_BATCH_ALARM_NAME,
            topic_arn=release_batch_sns_topic_info["arn"],
            queue_name=RELEASE_BATCH_QUEUE_NAME,
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
        s3.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={
                "LocationConstraint": settings.AWS_REGION,
            },
            ACL="private",
        )
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
        "arn:aws:iam::aws:policy/AWSLambda_FullAccess",
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
    max_cuncurrent_executions: int = 40,
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
                ScalingConfig={
                    "MaximumConcurrency": max_cuncurrent_executions,
                },
            )

        logger.info(
            f"Lambda function '{function_name}' created successfully"
            f"\n(ARN) {response['FunctionArn']}"
        )
        return {"arn": response["FunctionArn"]}

    except Exception as e:
        logger.error(f"Error creating Lambda function: {e}")
        return None


def get_sns_topic(topic_name: str) -> dict | None:
    sns_client = boto3.client("sns")
    try:
        response = sns_client.list_topics()
        for topic in response.get("Topics", []):
            if topic_name in topic["TopicArn"]:
                logger.info(
                    f"SNS topic '{topic_name}' exists" f"\n(ARN) {topic['TopicArn']}"
                )
                return {"arn": topic["TopicArn"]}
        logger.info(f"SNS topic '{topic_name}' does not exist.")
        return None

    except Exception as e:
        logger.error(f"Error getting SNS topic: {e}")
        return None


def create_sns_topic(topic_name: str) -> dict | None:
    sns_client = boto3.client("sns")
    try:
        response = sns_client.create_topic(Name=topic_name)
        logger.info(
            f"SNS topic '{topic_name}' created successfully"
            f"\n(ARN) {response['TopicArn']}"
        )
        return {"arn": response["TopicArn"]}

    except Exception as e:
        logger.error(f"Error creating SNS topic: {e}")
        return None


def get_sns_lambda_subscription(topic_arn: str, lambda_arn: str) -> dict | None:
    sns_client = boto3.client("sns")
    try:
        response = sns_client.list_subscriptions_by_topic(TopicArn=topic_arn)
        for subscription in response.get("Subscriptions", []):
            if (
                subscription["Protocol"] == "lambda"
                and subscription["Endpoint"] == lambda_arn
            ):
                logger.info(
                    f"Lambda '{lambda_arn}' subscription to SNS topic '{topic_arn}' exists"
                    f"\n(Subscription ARN) {subscription['SubscriptionArn']}"
                )
                return {"arn": subscription["SubscriptionArn"]}
        logger.info(
            f"Lambda '{lambda_arn}' subscription to SNS topic '{topic_arn}' does not exist."
        )
        return None

    except Exception as e:
        logger.error(f"Error getting SNS subscription: {e}")
        return None


def subscribe_lambda_to_sns_topic(topic_arn: str, lambda_arn: str) -> dict | None:
    sns_client = boto3.client("sns")
    try:
        response = sns_client.subscribe(
            TopicArn=topic_arn,
            Protocol="lambda",
            Endpoint=lambda_arn,
        )
        logger.info(
            f"Subscribed Lambda '{lambda_arn}' to SNS topic '{topic_arn}' successfully"
            f"\n(ARN) {response['SubscriptionArn']}"
        )
        return {"arn": response["SubscriptionArn"]}

    except Exception as e:
        logger.error(f"Error subscribing Lambda to SNS topic: {e}")
        return None


def add_lambda_sns_permission(function_name: str, topic_arn: str) -> bool:
    lambda_client = boto3.client("lambda")
    statement_id = f"{function_name}-sns-permission"
    try:
        lambda_client.add_permission(
            FunctionName=function_name,
            StatementId=statement_id,
            Action="lambda:InvokeFunction",
            Principal="sns.amazonaws.com",
            SourceArn=topic_arn,
        )
        logger.info(
            f"Added SNS invoke permission to Lambda '{function_name}' for topic '{topic_arn}'."
        )
        return True

    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceConflictException":
            logger.info(
                f"SNS invoke permission for Lambda '{function_name}' and topic '{topic_arn}' exists."
            )
            return True
        else:
            logger.error(f"Error adding SNS permission to Lambda: {e}")
            return False


def get_cloudwatch_alarm(alarm_name: str) -> dict | None:
    cloudwatch_client = boto3.client("cloudwatch")
    try:
        response = cloudwatch_client.describe_alarms(AlarmNames=[alarm_name])
        if response.get("MetricAlarms", []):
            alarm = response["MetricAlarms"][0]
            logger.info(
                f"CloudWatch alarm '{alarm_name}' exists" f"\n(ARN) {alarm['AlarmArn']}"
            )
            return {"arn": alarm["AlarmArn"]}
        logger.info(f"CloudWatch alarm '{alarm_name}' does not exist.")
        return None

    except Exception as e:
        logger.error(f"Error getting CloudWatch alarm: {e}")
        return None


def create_cloudwatch_alarm(
    alarm_name: str, topic_arn: str, queue_name: str
) -> dict | None:
    cloudwatch_client = boto3.client("cloudwatch")
    try:
        response = cloudwatch_client.put_metric_alarm(
            AlarmName=alarm_name,
            AlarmDescription=f"Alarm when the SQS queue '{queue_name}' has been idle for 15 minutes",
            ActionsEnabled=True,
            AlarmActions=[topic_arn],
            MetricName="ApproximateNumberOfMessagesVisible",
            Namespace="AWS/SQS",
            Statistic="Maximum",
            Dimensions=[{"Name": "QueueName", "Value": queue_name}],
            Period=300,
            EvaluationPeriods=3,
            DatapointsToAlarm=3,
            Threshold=0,
            ComparisonOperator="LessThanOrEqualToThreshold",
            TreatMissingData="notBreaching",
        )
        logger.info(f"CloudWatch alarm '{alarm_name}' created successfully")
        return {"arn": response.get("AlarmArn", None)}

    except Exception as e:
        logger.error(f"Error creating CloudWatch alarm: {e}")
        return None


if __name__ == "__main__":
    main()
