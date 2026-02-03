from pydantic import BaseModel

from src.core.entities.release import Release


class QueueReleasePageBody(BaseModel):
    release: Release
    page_num: int
