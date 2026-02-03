from typing import List, Protocol
from io import BytesIO

from src.core.entities.metadata import MetaData


class ParserProvider(Protocol):
    def get_metadata_by_data(self, data: BytesIO) -> MetaData:
        """extract the metadata of a give file bytes"""
        ...

    def get_page_count(self, data: BytesIO) -> int:
        """get the page count of a file"""
        ...

    def split_pages(self, data: BytesIO) -> List[BytesIO]:
        """split multi-page file into single-page pdfs"""
        ...

    def extract_table_by_page_num(self, data: BytesIO,
                                  page_num: int
                                  ) -> List[List[str | None]]:
        """
            extract file page's table/s between
            and return list of string/none lists
        """
        ...
