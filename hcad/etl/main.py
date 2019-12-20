import argparse

from .. import __version__

parser = argparse.ArgumentParser(
    prog=__version__,
)
parser.add_argument("--debug", action="store_true")

if __name__ == "__main__":
    parser.parse_args()
