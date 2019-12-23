import csv
import io
import json
import logging
import re
import shutil
import subprocess
import tempfile
import zipfile
from collections import OrderedDict
from datetime import datetime as dt
from pathlib import Path
from typing import Iterator, List, Optional, TextIO, Tuple, Union
from urllib.parse import urljoin, urlsplit

log = logging.getLogger(__name__)

tax_year = dt.now().strftime("%Y")
db = Path("data/hcad/")
landing = db.joinpath("landing")
staging = db.joinpath("staging")

dictionary = re.findall(
    r"(\w+)\s+(\w+)\s+(\d+)\s?",
    db.joinpath("Desc")
    .joinpath(tax_year)
    .joinpath("Layout_and_Length.txt")
    .read_text(),
)


def get_fields(table: str) -> List[str]:
    return [i[1] for i in dictionary if i[0] in table]


def get_max_columns(table: str) -> int:
    return len([i for i in dictionary if i[0] in table])


def get_field_size_limit(table: str) -> int:
    return max(int(i[-1]) for i in dictionary if i[0] in table)


def process_zip(*files: Path) -> Iterator[Path]:
    tmp = Path(tempfile.mkdtemp())
    for file in files:
        dst = tmp.joinpath(file.parent.name).joinpath(file.stem)
        dst.mkdir(parents=True, exist_ok=True)
        log.info("Processing %s" % file)
        zip_file = zipfile.ZipFile(file)
        zip_file.printdir()
        zip_file.extractall(dst)
        for extract in dst.rglob("*.txt"):
            yield extract
        log.info("Processed %s" % file)


def process_txt(*files: Path) -> Iterator[Path]:
    for file in files:
        dst = (
            staging.joinpath(file.parent.parent.name)
            .joinpath(file.parent.name)
            .joinpath(file.name)
            .with_suffix(".csv")
        )
        dst.parent.mkdir(parents=True, exist_ok=True)
        fields = get_fields(file.stem)
        csv.field_size_limit(get_field_size_limit(file.stem))
        if not dst.exists():
            with file.open(encoding="iso-8859-1", newline="") as fin:
                reader = csv.DictReader(
                    fin, fieldnames=fields, dialect="excel-tab",
                )

                with dst.open("w+") as fout:
                    writer = csv.DictWriter(fout, fieldnames=fields)
                    writer.writeheader()
                    try:
                        for row in reader:
                            writer.writerow(row)
                    except csv.Error as csv_error:
                        log.error(csv_error)
                    yield dst
        file.unlink()


def land(year: Optional[str] = None) -> Iterator[Path]:
    log.info("Landing %s", year)
    cmd = f"hcad-land.sh {year}"
    yield from landing.rglob(f"**/{year}/**/*.zip")


def stage(*landed) -> Iterator[Path]:
    yield from process_txt(*process_zip(*landed))
