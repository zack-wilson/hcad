import argparse
import csv
import json
import logging
import os
import re
from datetime import datetime as dt
from pathlib import Path
from typing import Optional, Tuple
from urllib.parse import urljoin
from zipfile import ZipFile

import requests
import time
# import pandas as pd
from pyspark.sql import SparkSession

# from pyspark.sql.functions import *

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

TAX_YEAR = dt.now().strftime("%Y")
DOMAIN = "https://pdata.hcad.org"
DICTIONARY = Path("Desc/Layout_and_Length.txt")
REMOTE = Path("data/cama/")
LANDING = Path("samples/landing/")
STAGING = Path("samples/staging/")
RELEASE = Path("samples/release/")

spark = (
    SparkSession.builder.appName("Etl")
        .config("spark.sql.warehouse.dir", STAGING.joinpath("spark-warehouse").as_posix())
        .getOrCreate()
)


def dictionary(table: str) -> Tuple[str, ...]:
    src = urljoin(DOMAIN, DICTIONARY.as_posix())
    dst = LANDING.joinpath(DICTIONARY)
    if not dst.exists():
        response = requests.get(src)
        try:
            dst.parent.mkdir(parents=True)
        except FileExistsError:
            pass
        dst.write_bytes(response.content)
    return tuple(
        filter(
            lambda x: table in x, re.findall(r"(\w+)\s+(\w+)\s+(\d+)", dst.read_text())
        )
    )


def land(archive: str, tax_year: str = TAX_YEAR, landing: Path = LANDING) -> None:
    source = urljoin(DOMAIN, (REMOTE / tax_year / archive).as_posix())
    target = landing / tax_year / archive.rstrip(".zip")
    if not target.exists():
        log.info("Downloading %s" % source)
        response = requests.get(source)
        try:
            target.mkdir(parents=True)
        except FileExistsError:
            pass
        target.joinpath(archive).write_bytes(response.content)
    with ZipFile(target.joinpath(archive)) as zf:
        log.info("Unpacking %s", zf)
        zf.extractall(target / "raw")
    for raw in target.joinpath("raw").rglob("*.txt"):
        log.info("Mixing %s", raw)
        with raw.open(encoding="iso-8859-1", newline="") as fin:
            field_names = tuple(i[1] for i in dictionary(raw.stem))
            log.info("Mixing %s", fin)
            reader = csv.DictReader(
                fin, fieldnames=tuple(field_names), dialect="excel-tab"
            )
            with raw.with_suffix(".csv").open(mode="w+", newline="") as fout:
                log.info("Prepped %s", fout)
                writer = csv.DictWriter(fout, fieldnames=field_names)
                writer.writeheader()
                writer.writerows(reader)
        raw.unlink()
    for cooked in target.joinpath("raw").rglob("*.csv"):
        log.info("Adding sprinkles %s", cooked)
        dst = target / cooked.name.replace(".csv", ".parquet")
        dd = dictionary(cooked.stem)
        max_columns = len(dd)
        max_chars_per_column = max(int(i[-1]) for i in dd)
        log.info(
            json.dumps(
                {
                    dst.as_posix(): dict(
                        max_columns=max_columns,
                        max_chars_per_column=max_chars_per_column,
                    )
                },
                indent="    ",
            )
        )
        df = spark.read.csv(
            os.path.realpath(cooked),
            header=True,
            dateFormat="MM/dd/yyyy",
            maxColumns=max_columns,
            # maxCharsPerColumn=max_chars_per_column,
        )
        df.explain()
        df.printSchema()
        df.show()
        time.sleep(3)
        df.write.mode("overwrite").parquet(os.path.realpath(dst))
        cooked.unlink()
    target.joinpath("raw").rmdir()


def run(*archives, tax_year: str, debug: Optional[bool]):
    if debug:
        log.setLevel(debug)
    for archive in archives:
        land(archive, tax_year=tax_year)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--debug", action="store_true")
    parser.add_argument("-y", "--year", default=TAX_YEAR)
    parser.add_argument("archives", type=str, nargs="+")
    args = parser.parse_args()
    start = dt.now()
    run(*args.archives, tax_year=args.year, debug=args.debug)
    finish = dt.now()
    log.info("start=%s,finish=%s,elapsed=%s", start, finish, finish - start)


if __name__ == "__main__":
    main()
