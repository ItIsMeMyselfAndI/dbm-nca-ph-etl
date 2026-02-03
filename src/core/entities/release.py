from typing import Optional
from pydantic import BaseModel


class Release(BaseModel):
    id: str
    title: str
    url: str
    filename: str
    year: int
    page_count: int = 0
    file_meta_created_at: Optional[str] = None
    file_meta_modified_at: Optional[str] = None
