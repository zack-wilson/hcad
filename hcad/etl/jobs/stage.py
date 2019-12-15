import argparse
import csv
import json
import logging
import os
import re
from pathlib import Path
from typing import Iterator, Tuple
from urllib.parse import urljoin, urlsplit
from zipfile import ZipFile

from ..settings import DICTIONARY, LANDING, STAGING, TAX_YEAR
from . import land

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
logging_format = "%(asctime)s:%(name)s:%(funcName)s - %(message)s"
logging.basicConfig(level=logging.INFO, format=logging_format)


def run(year: str, debug=False):
    if debug:
        log.setLevel(logging.DEBUG)
    dictionary = LANDING.joinpath(DICTIONARY)
    sources: Tuple[Path, ...] = tuple(LANDING.rglob(f"**/{year}/*.zip"))
    log.debug(sources)

    try:
        os.makedirs((os.path.dirname(STAGING.joinpath(DICTIONARY))))
    except FileExistsError:
        pass

    try:
        with STAGING.joinpath("Desc/").joinpath("dictionary.txt").open(
            mode="w+", encoding="iso-8859-1"
        ) as f:
            print("file", "field", "length", sep=",", file=f)
            for i in re.findall(
                r"(\w+)\s+(\w+)\s+(\d+)\s?",
                LANDING.joinpath(DICTIONARY).read_text(),
            ):
                print(*i, sep=",", file=f)
    except FileNotFoundError as file_not_found_error:
        log.error(file_not_found_error)

    for src in sources:
        dst = STAGING.joinpath(year).joinpath(src.stem)
        log.info(dst)
        # run landing first
        land.run(src.name, year=year, debug=debug)
        log.info("Staging: %s", src)

        try:
            os.makedirs(dst.as_posix())
            log.info("Created directories: %s", dst)
        except FileExistsError:
            log.warning("Path exists: %s", dst)

        with ZipFile(src) as zip_file:
            zip_file.printdir()
            zip_file.extractall(dst)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--debug", action="store_true")
    parser.add_argument("-y", "--year", default=TAX_YEAR)
    args = parser.parse_args()
    run(year=args.year, debug=args.debug)


if __name__ == "__main__":
    main()
