import argparse
import csv
import io
import logging
import os
import re
from collections import OrderedDict
from datetime import datetime as dt
from pathlib import Path
from typing import Sequence
from zipfile import ZipFile

from ..settings import DATABASE, LANDING, STAGING
from ..main import root_parser

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
logging_format = "%(asctime)s:%(name)s:%(funcName)s - %(message)s"
logging.basicConfig(level=logging.INFO, format=logging_format)


def read_csv(file, fieldnames: Sequence[str], dialect: str):
    reader = csv.DictReader(file, fieldnames=fieldnames, dialect=dialect)
    try:
        yield from reader
    except csv.Error as csv_error:
        log.error(csv_error)


def write_csv(*rows: OrderedDict, dst: Path, fieldnames: Sequence[str]):
    with dst.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        try:
            writer.writeheader()
            for row in rows:
                writer.writerow(row)
                yield row
            dst.with_name("_SUCCESS").touch(exist_ok=True)
        except csv.Error as csv_error:
            log.debug(csv_error)


def stage(*sources: Path):
    for src in sources:
        log.info("Processing %s", src.name)
        with ZipFile(src) as zip_file:
            for path in zip_file.infolist():
                if not path.is_dir():
                    log.info("Extracting %s from %s", path.filename, path)
                    with zip_file.open(path) as f:
                        field_names = [
                            i[1]
                            for i in re.findall(
                                r"(\w+)\s+(\w+)\s+(\d+)",
                                DATABASE.joinpath("Desc")
                                .joinpath(src.parent.name)
                                .joinpath("Layout_and_Length.txt")
                                .read_text(),
                            )
                            if i[0] in f.name
                        ]
                        log.debug(field_names)
                        dst = (
                            STAGING.joinpath(src.parent.name)
                            .joinpath(src.stem)
                            .joinpath(f.name)
                            .with_suffix(".csv")
                        )
                        if dst.exists():
                            pass
                        else:
                            try:
                                os.makedirs(dst.parent.as_posix())
                            except FileExistsError:
                                pass
                            log.info("Reading %s", f)
                            rows = read_csv(
                                io.TextIOWrapper(
                                    f, encoding="iso-8859-1", newline=""
                                ),
                                fieldnames=field_names,
                                dialect="excel-tab",
                            )
                            log.info("Writing %s", dst)
                            write_csv(*rows, dst=dst, fieldnames=field_names)


def run(debug=False):
    sources = tuple(LANDING.rglob(f"**/*.zip"))
    start_time = dt.now()
    log.info("Running: %s", start_time)
    if debug:
        log.setLevel(logging.DEBUG)
    log.info("Debug is %s", debug and "on" or "off")
    stage(*sources)
    end_time = dt.now()
    log.info(
        f"start=%s, end=%s, elapsed=%s",
        start_time,
        end_time,
        end_time - start_time,
    )
    log.info("Done.")


def main():
    parser = argparse.ArgumentParser(parents=[root_parser], add_help=False)
    parser.add_argument(
        "--year",
        choices=[*range(2005, dt.now().year + 1)],
        default=dt.now().strftime("%Y"),
        nargs="+",
        type=str,
    )
    args = parser.parse_args()
    run(debug=args.debug)


if __name__ == "__main__":
    main()
