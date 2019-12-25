import argparse
import logging
from configparser import ConfigParser
from datetime import datetime as dt


from hcad import settings, __version__
from hcad.etl import run

logging.basicConfig(level=logging.INFO, format=settings.log_fmt)

config = ConfigParser(
    dict(
        tax_year=dt.now().strftime("%Y"),
        landing=settings.db.joinpath("landing").as_posix(),
        staging=settings.db.joinpath("staging").as_posix(),
    )
)


def main():
    parser = argparse.ArgumentParser(prog="hcad")
    parser.add_argument("--version", action="version", version=__version__)
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
        landing=args.landing,
        staging=args.staging,
        debug=args.debug,
    )


if __name__ == "__main__":
    main()
