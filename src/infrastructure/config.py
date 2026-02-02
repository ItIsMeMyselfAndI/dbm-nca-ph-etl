from typing import Optional
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    SUPABASE_URL: str
    SUPABASE_ANON_KEY: str

    AWS_PROFILE: Optional[str] = None
    AWS_REGION: Optional[str] = "ap-southeast-1"

    SQS_QUEUE_URL: Optional[str] = None
    S3_BUCKET_NAME: Optional[str] = None

    # AWS_ACCESS_KEY_ID: Optional[str] = None
    # AWS_SECRET_ACCESS_KEY: Optional[str] = None

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        extra = "ignore"


settings = Settings()  # pyright:  ignore
