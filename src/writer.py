import logging
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import polars as pl


class PolarsWriter:
    def __init__(self, output_dir: str, report_start_date: date = None, report_end_date: date = None):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.report_start_date = report_start_date.isoformat() if report_start_date else None
        self.report_end_date = report_end_date.isoformat() if report_end_date else None

    def write_csv(self, df: pl.DataFrame, file_name: str):
        processed_file_name = self.__process_file_name(file_name)
        path = self.output_dir / processed_file_name
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
            raise ValueError("File name can contain only one '.' that should be a part of file extension.")
        elif len(splitted_file_name) == 2:
            return f"{base_name}.{splitted_file_name[1]}"

        return f"{base_name}"


@dataclass(frozen=True)
class ReportRunLayout:
    root_dir: Path
    artifacts_dir: Path

    @classmethod
    def create(cls, base_output_dir: str, run_name: str) -> "ReportRunLayout":
        root_dir = Path(base_output_dir) / run_name
        artifacts_dir = root_dir / "artifacts"
        artifacts_dir.mkdir(parents=True, exist_ok=True)
        return cls(root_dir=root_dir, artifacts_dir=artifacts_dir)

    def artifact_dir(self, section_name: str) -> Path:
        path = self.artifacts_dir / section_name
        path.mkdir(parents=True, exist_ok=True)
        return path

    def artifact_path(self, section_name: str, file_name: str) -> Path:
        path = self.artifact_dir(section_name) / file_name
        path.parent.mkdir(parents=True, exist_ok=True)
        return path

    def writer(self, section_name: str, report_start_date: date, report_end_date: date) -> PolarsWriter:
        return PolarsWriter(
            output_dir=str(self.artifact_dir(section_name)),
            report_start_date=report_start_date,
            report_end_date=report_end_date,
        )

    def pdf_path(self, file_name: str) -> Path:
        self.root_dir.mkdir(parents=True, exist_ok=True)
        return self.root_dir / file_name
