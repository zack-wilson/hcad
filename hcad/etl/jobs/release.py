import argparse
import csv
import logging
import os

from .. import settings

log = logging.getLogger(__name__)
logging_format = (
    "%(asctime)s:%(levelname)s::%(name)s:%(funcName)s - %(message)s"
)
logging.basicConfig(level=logging.INFO, format=logging_format)


def run(year, debug: bool = False):
    if debug:
        log.setLevel(logging.DEBUG)
    log.info("Debug is %s", debug and "on" or "off")
    dictionary = settings.STAGING / "Desc" / "dictionary.txt"
    source = settings.STAGING / year

    for src in source.rglob("*.txt"):
        log.info("Processing: %s", src)
        dst = settings.RELEASE / year / src.parent.name / f"{src.stem}.csv"
        try:
            os.makedirs(os.path.dirname(dst))
        except FileExistsError:
            pass

        field_names = [
            i.split(",")[1]
            for i in dictionary.read_text().splitlines()
            if src.stem == i.split(",")[0]
        ]
        log.debug(field_names)
        log.info("Reading: %s", src)
        with src.open(newline="", encoding="iso-8859-1") as f:
            reader = csv.DictReader(
                f, fieldnames=field_names, dialect="excel-tab"
            )
            log.info("Writing: %s", dst)
            with open(dst, "w+", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=field_names)
                writer.writeheader()
                try:
                    writer.writerows(reader)
                except Exception as exception:
                    log.error(exception)
        print(dst)
    else:
        log.warning("No sources found in %s", source)
    log.info("Done.")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--debug", action="store_true")
    parser.add_argument("-y", "--year", default=settings.TAX_YEAR)
    args = parser.parse_args()
    run(year=args.year, debug=args.debug)


if __name__ == "__main__":
    main()
