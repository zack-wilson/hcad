import argparse
import logging
import os
import re
import subprocess
import tempfile
from configparser import ConfigParser
from datetime import datetime as dt
from pathlib import Path
from typing import Generator, Iterator, Optional, Tuple
from zipfile import ZipFile

import hcad
from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.utils import AnalysisException
from pyspark.util import Py4JJavaError

log = logging.getLogger(__name__)
db = Path("data/hcad/")
config = ConfigParser(
    dict(
        tax_year=dt.now().strftime("%Y"),
        landing=db.joinpath("landing").as_posix(),
        staging=db.joinpath("staging").as_posix(),
    )
)

dictionary = re.findall(
    r"(\w+)\s+(\w+)\s+(\d+)\s?",
    db.joinpath("Desc")
    .joinpath(config.defaults()["tax_year"])
    .joinpath("Layout_and_Length.txt")
    .read_text(),
)

spark = SparkSession.builder.getOrCreate()


def get_fields(table: str) -> Tuple:
    return tuple(i[1] for i in dictionary if i[0] in table)


def get_max_columns(table: str) -> int:
    return len([i for i in dictionary if i[0] in table])


def get_max_chars_per_column(table: str) -> int:
    return max(int(i[-1]) for i in dictionary if i[0] in table)


def land(year: Optional[str] = None) -> subprocess.CompletedProcess:
    log.info("Landing %s", year)
    return subprocess.run(f"hcad-land.sh {year}".split(), check=True)


def extract() -> Iterator[Path]:
    for path in sorted(
        Path(config.defaults()["landing"]).rglob(
            f"**/{config.defaults()['tax_year']}/**/*.zip"
        )
    ):
        tmp = Path(tempfile.mkdtemp())
        print("Extracting %s to %s" % (path, tmp))
        with ZipFile(path) as zip_file:
            log.info("%s to %s", zip_file, tmp)
            zip_file.extractall(tmp.joinpath(path.stem))
        for f in tmp.rglob("*.txt"):
            yield f
            print("Extracted %s from %s", f, path)


def transform(*args: Path) -> DataFrame:
    for f in args:
        print("Transforming %s", f)
        fields = get_fields(f.stem)
        max_columns = round(get_max_columns(f.stem))
        max_chars_per_column = round(get_max_chars_per_column(f.stem) * 1.5)
        log.debug(
            "max_columns=%s,max_chars_per_columns=%s,fields=%s",
            max_columns,
            max_chars_per_column,
            fields,
        )
        try:
            df = spark.read.csv(
                f.as_posix(),
                encoding="iso-8859-1",
                sep="\t",
                header=False,
                maxColumns=max_columns,
                maxCharsPerColumn=max_chars_per_column,
                ignoreTrailingWhiteSpace=True,
                ignoreLeadingWhiteSpace=True,
            )

            for i, name in zip(df.columns, fields):
                log.debug("Renaming col %s as %s", i, name)
                df = df.withColumnRenamed(i, name)
            yield f, df
            print("Transformed %s.", f)
        except Py4JJavaError as py4J_java_error:
            print("Transformation Failed.")
            log.error(py4J_java_error)
        except AnalysisException as analysis_exception:
            print("Transformation Failed.")
            log.debug(analysis_exception)


def load(*args: DataFrame) -> None:
    for arg in args:
        f, df = arg
        log.info("Loading %s", df)
        dst = (
            Path(config.defaults()["staging"])
            .joinpath(config.defaults()["tax_year"])
            .joinpath(f.parent.name)
            .joinpath(f.name)
            .with_suffix(".csv")
        )
        try:
            dst.mkdir(parents=True)
        except FileExistsError:
            pass
        df.show()
        try:
            df.write.mode("overwrite").csv(
                dst.as_posix(), header=True, compression="gzip"
            )
            print("Loaded %s" % dst)
        except Py4JJavaError as py4j_java_error:
            print("Load failed.")
            log.error(py4j_java_error)
        except AnalysisException as analysis_exception:
            log.debug(analysis_exception)

        os.unlink(f)


def stage() -> None:
    log.info("Staging")
    load(*transform(*extract()))
    log.info("Staging Completed at %s", dt.now())


def run(
    *years: str, landing: Path, staging: Path, debug: Optional[bool] = None
) -> None:
    if debug:
        log.setLevel(logging.DEBUG)
    log.info("Running")
    log.debug("landing=%s,staging=%s", landing, staging)
    for year in years:
        log.info("Processing in %s", year)
        result = land(year)
        log.debug(result)
    stage()


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
    args = parser.parse_args()
    run(
        *args.year,
        landing=args.landing,
        staging=args.staging,
        debug=args.debug,
    )


if __name__ == "__main__":
    main()
