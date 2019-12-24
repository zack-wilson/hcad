import csv
import gzip
import logging
import shutil
import subprocess
import tempfile
import zipfile
from pathlib import Path
from typing import Iterator, Optional, Union

from . import functions, settings

log = logging.getLogger(__name__)

context = dict(
    tax_year=settings.tax_year,
    landing=settings.db.joinpath("landing"),
    staging=settings.db.joinpath("staging"),
)


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
        with file.open("rb") as f_in:
            log.info("Compressing %s", f_in)
            dst = file.with_suffix(f"{file.suffix}.gz")
            with gzip.open(dst, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)
                log.info("Compressed %s", f_out)
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
            settings.staging.joinpath(file.parent.parent.name)
            .joinpath(file.parent.name)
            .joinpath(file.name)
            .with_suffix(".csv")
        )
        dst.parent.mkdir(parents=True, exist_ok=True)
        fields = functions.get_fields(file.stem)
        csv.field_size_limit(functions.get_field_size_limit(file.stem))
        if not dst.exists():
            with file.open(encoding="iso-8859-1", newline="") as f_in:
                reader = csv.DictReader(
                    f_in, fieldnames=fields, dialect="excel-tab",
                )

                with dst.open("w+") as f_out:
                    writer = csv.DictWriter(f_out, fieldnames=fields)
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
    subprocess.run(cmd.split())
    yield from settings.landing.rglob(f"**/{year}/**/*.zip")


def stage(*landed) -> Iterator[Path]:
    yield from compress_csv(*process_txt(*decompress_zip(*landed)))
