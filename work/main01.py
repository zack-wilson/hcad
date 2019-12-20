import argparse
import logging
import os
from configparser import ConfigParser
from pathlib import Path
from typing import Iterator, Optional

from pyspark.files import SparkFiles
from pyspark.sql import SparkSession, functions as F, types as T
from pyspark.util import Py4JJavaError
from pyspark.sql.utils import *


log = logging.getLogger(os.path.dirname(os.curdir))
config = ConfigParser()


def run(debug: Optional[bool] = None, config: Optional[ConfigParser] = None) -> None:
    if debug:
        log.setLevel(logging.DEBUG)
    print(f"Debug is {debug and 'on' or 'off'}")

        
def main() -> None:
    config = ConfigParser()
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", type=str, default="hcad.cfg")
    parser.add_argument("--debug", action='store_true')
    args = parser.parse_args()
    
    print(f'Loading configuration from file: {args.config}')
    config.read([args.config, "fart.cfg"])

    debug = args.debug or config.getboolean("run", "debug", fallback=False)
    run(debug=debug, config=config)
    

if __name__ == "__main__":
    main()
