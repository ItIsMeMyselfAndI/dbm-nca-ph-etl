from typing import List
from pydantic import BaseModel


class RawPageTable(BaseModel):
    rows: List[List[str | None]]
    page_num: int
