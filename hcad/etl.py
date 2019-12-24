import csv
import gzip
import io
import json
import logging
import os
import re
import shelve
import shutil
import subprocess
import tempfile
import zipfile
from collections import OrderedDict
from datetime import datetime as dt
from pathlib import Path
from typing import Iterator, List, Optional, TextIO, Tuple, Union
from urllib.parse import urljoin, urlsplit

import requests

log = logging.getLogger(__name__)

cache = shelve.open(".cache")
db = Path("data/hcad/")
context = dict(
    tax_year=dt.now().strftime("%Y"),
    db=Path("data/hcad/"),
    landing=db.joinpath("landing"),
    staging=db.joinpath("staging"),
)

tax_year = dt.now().strftime("%Y")
landing = db.joinpath("landing")
staging = db.joinpath("staging")

if not cache.get("DICTIONARY"):
    cache["DICTIONARY"] = requests.get(
        "https://pdata.hcad.org/Desc/Layout_and_Length.txt"
    )

dictionary = re.findall(r"(\w+)\s+(\w+)\s+(\d+)\s?", cache["DICTIONARY"].text)


def get_fields(table: str) -> List[str]:
    return [i[1] for i in dictionary if i[0] in table]


def get_max_columns(table: str) -> int:
    return len([i for i in dictionary if i[0] in table])


def get_field_size_limit(table: str) -> int:
    return max(int(i[-1]) for i in dictionary if i[0] in table)


def decompress_zip(*files: Union[str, Path]) -> Iterator[Path]:
    tmp = Path(tempfile.mkdtemp())
    for file in map(Path, files):
        dst = tmp.joinpath(file.parent.name).joinpath(file.stem)
        dst.mkdir(parents=True, exist_ok=True)
        log.info("Processing %s" % file)
        zip_file = zipfile.ZipFile(file)
        zip_file.printdir()
        zip_file.extractall(dst)
        for extract in dst.rglob("*.txt"):
            yield extract
        log.info("Processed %s" % file)


def compress_csv(*files: Union[Path, str]) -> Iterator[Path]:
    for file in map(Path, files):
        with file.open("rb") as fin:
            log.info("Compressing %s", fin)
            dst = file.with_suffix(f"{file.suffix}.gz")
            with gzip.open(dst, "wb") as fout:
                shutil.copyfileobj(fin, fout)
                log.info("Compressed %s", fout)
                yield dst
        file.unlink()


def decompress_gzip(*files: Union[Path, str]) -> Iterator[Path]:
    for file in map(Path, files):
        tmp = Path(tempfile.mkdtemp())
        dst = tmp.joinpath(file.stem).with_suffix(".txt")
        with gzip.open(file) as f:
            dst.write_bytes(f.read())
            yield dst


def process_txt(*files: Union[Path, str]) -> Iterator[Path]:
    for file in map(Path, files):
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
    yield from compress_csv(*process_txt(*decompress_zip(*landed)))
