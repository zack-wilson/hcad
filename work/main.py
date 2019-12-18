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
from pyspark.sql import SparkSession
from pyspark.util import Py4JJavaError

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

TAX_YEAR = dt.now().strftime("%Y")
DOMAIN = "https://pdata.hcad.org"
DICTIONARY = Path("Desc/Layout_and_Length.txt")
REMOTE = Path("data/cama/")
LANDING = Path("samples/landing/")
STAGING = Path("samples/staging/")
RELEASE = Path("samples/release/")


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


def land(
        archive: str,
        tax_year: str = TAX_YEAR,
        landing: Path = LANDING,
        rel_tol: float = 1.6,
) -> None:
    spark = (
        SparkSession.builder.appName(f"Landing_{tax_year}")
            .config(
            "spark.sql.warehouse.dir", STAGING.joinpath("spark-warehouse").as_posix()
        )
            .getOrCreate()
    )
    source = urljoin(DOMAIN, (REMOTE / tax_year / archive).as_posix())
    target = landing / tax_year / archive.rstrip(".zip")
    if not target.exists():
        log.info("Bagging %s" % source)
        response = requests.get(source)
        try:
            target.mkdir(parents=True)
        except FileExistsError:
            pass
        target.joinpath(archive).write_bytes(response.content)
    with ZipFile(target.joinpath(archive)) as zf:
        log.info("Unwrapping %s", zf)
        zf.extractall(target / "raw")
    for raw in target.joinpath("raw").rglob("*.txt"):
        if not raw.parent.with_suffix(".parquet").exists():
            with raw.open(encoding="iso-8859-1", newline="") as fin:
                field_names = tuple(i[1] for i in dictionary(raw.stem))
                log.info("Mixing %s + %s", field_names, fin)
                reader = csv.DictReader(
                    fin, fieldnames=tuple(field_names), dialect="excel-tab"
                )
                try:
                    with raw.with_suffix(".csv").open(mode="w+", newline="") as fout:
                        writer = csv.DictWriter(fout, fieldnames=field_names)
                        writer.writeheader()
                        writer.writerows(reader)
                        log.info("Baked %s", fout)
                except csv.Error as csv_error:
                    log.error(csv_error)
                    log.info("Trashed %s", writer)
            raw.unlink()
    for prepped in target.joinpath("raw").rglob("*.csv"):
        log.info("Adding sprinkles %s", prepped)
        dst = target / prepped.name.replace(".csv", ".parquet")
        dd = dictionary(prepped.stem)
        max_columns = len(dd)
        max_chars_per_column = int(round(max(int(i[-1]) * rel_tol for i in dd)))
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
        try:
            df = spark.read.csv(
                os.path.realpath(prepped),
                header=True,
                dateFormat="MM/dd/yyyy",
                maxColumns=max_columns,
                maxCharsPerColumn=max_chars_per_column,
            )
            df.explain()
            df.printSchema()
            df.show()
            time.sleep(3)
            df.write.mode("overwrite").parquet(os.path.realpath(dst))
            prepped.unlink()
        except Py4JJavaError as py4j_java_error:
            log.error(py4j_java_error)
            log.info("Trashing %s", prepped)


def run(*archives: str, tax_year: str, debug: Optional[bool] = False):
    if debug:
        log.setLevel(debug)
    for archive in archives:
        land(archive=archive, tax_year=tax_year)


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
