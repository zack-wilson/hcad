import argparse
import csv
import io
import json
import logging
import os
import re
from collections import OrderedDict, namedtuple
from datetime import datetime as dt
from pathlib import Path
from typing import (Any, Dict, Generator, Iterator, Optional, Sequence, TextIO,
                    Tuple, Union)
from urllib.parse import urljoin, urlsplit
from zipfile import ZipFile, ZipInfo

from ..settings import DATABASE, LANDING, STAGING

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
logging_format = "%(asctime)s:%(name)s:%(funcName)s - %(message)s"
logging.basicConfig(level=logging.INFO, format=logging_format)


def stage(*sources):
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
                            .joinpath("csv")
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
                            reader = csv.DictReader(
                                io.TextIOWrapper(
                                    f, encoding="iso-8859-1", newline=""
                                ),
                                fieldnames=field_names,
                                dialect="excel-tab",
                            )
                            log.info("Writing %s", dst)
                            with dst.open(
                                "w", newline="", encoding="utf-8"
                            ) as f:
                                writer = csv.DictWriter(
                                    f, fieldnames=field_names
                                )
                                try:
                                    writer.writeheader()
                                    for row in reader:
                                        writer.writerow(row)
                                    dst.with_name("_SUCCESS").touch(
                                        exist_ok=True
                                    )
                                except csv.Error as csv_error:
                                    log.debug(csv_error)


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
    parser = argparse.ArgumentParser()
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()
    run(debug=args.debug)


if __name__ == "__main__":
    main()
