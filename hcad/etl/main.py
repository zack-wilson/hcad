import argparse
import logging

from . import settings
from .jobs import land, release, stage

_log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--debug", action="store_true")
    parser.add_argument("-y", "--year", default=settings.TAX_YEAR)
    parser.add_argument("archives", type=str, nargs="+")
    args = parser.parse_args()

    land.run(year=args.year, debug=args.debug, *args.archives)
    stage.run(year=args.year, debug=args.debug)
    release.run(year=args.year, debug=args.debug)


if __name__ == "__main__":
    main()
