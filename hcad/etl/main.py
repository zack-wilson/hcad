import argparse
from datetime import datetime as dt
from .jobs import land, stage
from .. import __version__

root_parser = argparse.ArgumentParser(prog="hcad", add_help=False)
root_parser.add_argument("--version", action="version", version=__version__)
root_parser.add_argument("--debug", action="store_true")
root_parser.add_argument(
        "-y",
        "--year",
        type=int,
        choices=[*range(2005, dt.now().year + 1)],
        nargs="+",
        default=dt.now().strftime("%Y"),
    )
subparsers = root_parser.add_subparsers()

if __name__ == "__main__":
    args = root_parser.parse_args()
