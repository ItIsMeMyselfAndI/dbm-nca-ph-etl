from typing import List
from pydantic import BaseModel

from src.core.entities.release import Release


class PageRawTable(BaseModel):
    release: Release
    rows: List[List[str | None]]
    page_num: int
