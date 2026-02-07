from typing import Optional
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    SUPABASE_URL: str
    SUPABASE_ANON_KEY: str

    # AWS_PROFILE: Optional[str] = None
    AWS_REGION: str = "ap-southeast-1"

    AWS_S3_BUCKET_NAME: str

    AWS_SQS_RELEASE_QUEUE_URL: str
    AWS_SQS_RELEASE_PAGE_QUEUE_URL: str

    AWS_LAMBDA_FUNCTION_NAME: Optional[str] = None

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        extra = "ignore"


settings = Settings()  # pyright:  ignore
