import argparse
import csv
import logging
import os
import re
import subprocess
import tempfile
from collections import OrderedDict
from configparser import ConfigParser
from datetime import datetime as dt
from pathlib import Path
from typing import Generator, Iterator, List, Optional, Tuple
from zipfile import ZipFile

import hcad
from hcad import etl

log = logging.getLogger(__name__)
logfmt = "%(asctime)s:%(module)s:%(funcName)s: %(message)s"
logging.basicConfig(level=logging.INFO, format=logfmt)

db = Path("data/hcad/")


config = ConfigParser(
    dict(
        tax_year=dt.now().strftime("%Y"),
        landing=db.joinpath("landing").as_posix(),
        staging=db.joinpath("staging").as_posix(),
    )
)


def run(
    *years: str,
    archive: str,
    landing: Path,
    staging: Path,
    debug: Optional[bool] = None,
) -> None:
    if debug:
        log.setLevel(logging.DEBUG)
    log.info("Running")
    log.debug("landing=%s,staging=%s", landing, staging)
    for year in years:
        log.info("Processing in %s", year)
        print(*etl.stage(*etl.land(*years)))

def main():
    parser = argparse.ArgumentParser(prog="hcad")
    parser.add_argument(
        "--version", action="version", version=hcad.__version__
    )
    parser.add_argument("--debug", action="store_true")
    parser.add_argument(
        "year",
        type=int,
        choices=[*range(2005, int(config.defaults()["tax_year"]) + 1)],
        nargs="+",
    )
    parser.add_argument(
        "-l", "--landing", type=str, default=config.defaults()["landing"]
    )
    parser.add_argument(
        "-s", "--staging", type=str, default=config.defaults()["staging"]
    )
    parser.add_argument("-a", "--archive", type=str, default="*.zip")
    args = parser.parse_args()
    run(
        *args.year,
        archive=args.archive,
        landing=args.landing,
        staging=args.staging,
        debug=args.debug,
    )


if __name__ == "__main__":
    main()
