import boto3
from io import BytesIO
from botocore.exceptions import ClientError

from src.core.interfaces.storage import StorageProvider
from src.infrastructure.config import settings


class S3Storage(StorageProvider):
    def __init__(self, bucket_name: str, base_storage_path: str):
        self.s3 = boto3.client(
            's3',
            region_name=settings.AWS_REGION,
            # aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
            # aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
        )
        self.bucket_name = bucket_name
        self.base_storage_path = base_storage_path
        self._create_bucket()

    def get_filename_full_path(self, filename: str) -> str:
        if self.base_storage_path is None or self.base_storage_path == '':
            return filename
        if self.base_storage_path.endswith('/'):
            return f"{self.base_storage_path}{filename}"
        return f"{self.base_storage_path}/{filename}"

    def save_file(self, filename: str, data: BytesIO) -> None:
        data.seek(0)
        self._create_bucket()
        full_path = self.get_filename_full_path(filename)
        self.s3.upload_fileobj(data, self.bucket_name, full_path)

    def load_file(self, filename: str) -> BytesIO:
        full_path = self.get_filename_full_path(filename)
        response = self.s3.get_object(Bucket=self.bucket_name,
                                      Key=full_path)
        return BytesIO(response['Body'].read())

    def _create_bucket(self):
        try:
            self.s3.head_bucket(Bucket=self.bucket_name)
            print("Bucket exists!")
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                self.s3.create_bucket(Bucket=self.bucket_name)
                print("Bucket created.")
