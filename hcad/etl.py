import logging
import subprocess
from pathlib import Path
from typing import Iterator, Optional

from . import settings
from .functions import compress_csv, decompress_zip, process_txt

log = logging.getLogger(__name__)


class Context:
    tax_year = settings.tax_year
    landing = settings.db.joinpath("landing")
    staging = settings.db.joinpath("staging")


def land(year: Optional[str] = None) -> Iterator[Path]:
    log.info("Landing %s", year)
    cmd = f"_hcad_land.sh {year}"
    completed_process = subprocess.run(cmd.split())
    logging.info(completed_process)
    yield from settings.db.joinpath("landing").rglob(f"**/{year}/**/*.zip")


def stage(*landed) -> Iterator[Path]:
    yield from compress_csv(*process_txt(*decompress_zip(*landed)))


def run(
    *years: str, landing: Path, staging: Path, debug: Optional[bool] = None,
) -> None:
    if debug:
        log.setLevel(logging.DEBUG)
    log.info("Running")
    log.debug("landing=%s,staging=%s", landing, staging)
    for year in years:
        log.info("Processing in %s", year)
        print(*stage(*land(*years)))
