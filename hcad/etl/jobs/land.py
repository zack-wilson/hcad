import argparse
import json
import logging
import os
import subprocess
from datetime import datetime as dt
from typing import List, Optional, Union
from urllib.parse import urljoin, urlparse

from .. import services, settings

log = logging.getLogger(__name__)
logging_format = "%(asctime)s:%(name)s:%(funcName)s - %(message)s"
logging.basicConfig(level=logging.INFO, format=logging_format)


def run(years: Union[str, List[str]], debug: bool):
    if debug:
        log.setLevel(logging.DEBUG)
    log.info("Debug is %s", debug and "on" or "off")
    if isinstance(years, str):
        log.debug(subprocess.run(f"hcad-land.sh {years}".split()))
    else:
        for year in years:
            log.debug(subprocess.run(f"hcad-land.sh {year}".split()))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--debug", action="store_true")
    parser.add_argument(
        "--year",
        type=int,
        choices=[*range(2005, dt.now().year + 1)],
        nargs="+",
        default=dt.now().strftime("%Y"),
    )
    args = parser.parse_args()
    start = dt.now()
    log.info("Starting %s", start)
    run(args.year, debug=args.debug)
    end = dt.now()
    elapsed = end - start
    log.info("start=%s,end=%s,elapsed=%s", start, end, elapsed)
    log.info("Done.")


if __name__ == "__main__":
    main()
