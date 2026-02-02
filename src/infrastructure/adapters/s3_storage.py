import boto3
from io import BytesIO

from botocore.exceptions import ClientError
from src.core.interfaces.storage import StorageProvider
from src.infrastructure.config import settings


class S3Storage(StorageProvider):
    def __init__(self,  base_storage_path: str):
        self.s3 = boto3.client(
            's3',
            region_name=settings.AWS_REGION,
            # aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
            # aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
        )
        self.bucket_name = settings.S3_BUCKET_NAME
        self.base_storage_path = base_storage_path

    def get_base_storage_path(self):
        return self.base_storage_path

    def load_file(self, storage_path: str) -> BytesIO:
        response = self.s3.get_object(Bucket=self.bucket_name,
                                      Key=storage_path)
        return BytesIO(response['Body'].read())

    def save_file(self, storage_path: str, data: BytesIO) -> None:
        self.s3.upload_fileobj(data, self.bucket_name, storage_path)

    def _create_bucket(self):
        try:
            self.s3.head_bucket(Bucket=self.bucket_name)
            print("Bucket exists!")
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                self.s3.create_bucket(Bucket=self.bucket_name)
                print("Bucket created.")
