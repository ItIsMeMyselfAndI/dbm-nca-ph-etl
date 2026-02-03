from io import BytesIO
from typing import List
from PyPDF2 import PdfReader, PdfWriter
import pdfplumber
from pdfplumber.page import Page

from src.core.entities.metadata import MetaData
from src.core.interfaces.parser import ParserProvider
from src.infrastructure.constants import TABLE_COLUMNS


class PDFParser(ParserProvider):
    def __init__(self):
        self.table_settings = {
            "vertical_strategy": "explicit",
            "horizontal_strategy": "text",
            "explicit_vertical_lines": [],
            "intersection_tolerance": 1000,
            "snap_y_tolerance": 3,
            # "join_y_tolerance": 1,
        }
        pass

    def get_metadata_by_data(self, data: BytesIO) -> MetaData:
        reader = PdfReader(data)
        meta = reader.metadata
        metadata = MetaData(**{
            "created_at": meta.get('/CreationDate'),  # pyright: ignore
            "modified_at": meta.get('/ModDate')  # pyright: ignore
        })
        return metadata

    def get_page_count(self, data: BytesIO) -> int:
        reader = PdfReader(data)
        return len(reader.pages)

    def split_pages(self, data: BytesIO) -> List[BytesIO]:
        data_list: List[BytesIO] = []
        reader = PdfReader(data)
        for page in reader.pages:
            writer = PdfWriter()
            writer.add_page(page)

            page_buff = BytesIO()
            writer.write(page_buff)

            data_list.append(page_buff)

        return data_list

    def extract_table_by_page_num(self, data: BytesIO,
                                  page_num: int
                                  ) -> List[List[str | None]]:
        raw_rows: List[List[str | None]] = []

        with pdfplumber.open(data) as pdf:
            for i, page in enumerate(pdf.pages):
                if i == 0:
                    self._update_table_settings_vert_lines(page)

                if i != page_num:
                    continue

                rows = page.extract_table(self.table_settings)
                if rows:
                    raw_rows.extend(rows)
                # <test ----------->
                # self.display_page(page)
                # </test ----------->

        return raw_rows

    def display_page(self, page: Page):
        print(self.table_settings["explicit_vertical_lines"])
        im = page.to_image()
        im.debug_tablefinder(self.table_settings).show()

    def _update_table_settings_vert_lines(self, page: Page):
        target_phrases = TABLE_COLUMNS
        found_phrases = set()
        vert_lines: List[float] = []
        words = page.extract_words()
        texts = [w["text"] for w in words]
        for phrase in target_phrases:
            phrase_words = phrase.lower().split("_")
            n = len(phrase_words)
            for i in range(len(texts) - n + 1):
                curr_phrase = "_".join(texts[i:i+n]).lower()
                if curr_phrase == phrase:
                    vert_lines.append(words[i]["x0"])
                    found_phrases.add(phrase)
                    break
        page_right_side_x = page.width - 1
        vert_lines.append(page_right_side_x)
        self.table_settings["explicit_vertical_lines"] = vert_lines
