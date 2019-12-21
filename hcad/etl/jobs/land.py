import argparse
import logging
import subprocess

from typing import List, Union


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
