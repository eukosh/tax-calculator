import logging
import os
from datetime import date
from pathlib import Path

import polars as pl


class PolarsWriter:
    def __init__(self, output_dir: str, report_start_date: date = None, report_end_date: date = None):
        self.output_dir = output_dir
        # Check if the output directory exists; if not, create it
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
        self.report_start_date = report_start_date.isoformat() if report_start_date else None
        self.report_end_date = report_end_date.isoformat() if report_end_date else None

    def write_csv(self, df: pl.DataFrame, file_name: str):
        processed_file_name = self.__process_file_name(file_name)
        path = Path(self.output_dir) / processed_file_name
        df.write_csv(path)
        logging.info(f"Successfully wrote DataFrame to {path}")

    def __process_file_name(self, file_name: str):
        splitted_file_name = file_name.split(".")
        base_name = (
            f"{splitted_file_name[0]}__{self.report_start_date}_{self.report_end_date}"
            if self.report_start_date and self.report_end_date
            else splitted_file_name[0]
        )

        if len(splitted_file_name) > 2:
            raise ValueError("File name can contain only one '.' that is should be a part of file extension.")
        elif len(splitted_file_name) == 2:
            return f"{base_name}.{splitted_file_name[1]}"

        return f"{base_name}"
