import argparse
import json
import logging
import math
import os
from urllib.parse import urljoin, urlparse

from .. import services, settings

log = logging.getLogger(__name__)
logging_format = "%(asctime)s:%(name)s:%(funcName)s - %(message)s"
logging.basicConfig(level=logging.INFO, format=logging_format)


def run(*archives: str, year: str, debug: bool):
    log.info("Running.")
    if debug:
        log.setLevel(debug)
    log.info("Debug is %s", debug and "on" or "off")
    urls = [
        urljoin(settings.DOMAIN, p)
        for p in [
            settings.DICTIONARY,
            *[
                os.path.join(settings.REMOTE.format(year=year), s)
                for s in archives
            ],
        ]
    ]
    for url in urls:
        dst = settings.LANDING.joinpath(urlparse(url).path.lstrip("/"))
        try:
            os.makedirs(os.path.dirname(dst))
        except FileExistsError:
            pass
        if dst.exists():
            log.debug("File exists: %s", dst)
            log.debug("Analyzing %s for changes.", url)
            sizes = (
                services.http.head(url).headers.get("Content-Length"),
                dst.stat().st_size,
            )
            a, b = map(lambda x: int(x) if x is not None else 0, sizes)
            if not math.isclose(a, b, rel_tol=1.0):
                log.info("Detected changes to %s, downloading.", url)
                r = services.http.get(url)
                log.info(r.status_code)
                log.info(json.dumps(dict(r.headers), indent="    "))
                dst.write_bytes(r.content)
            else:
                log.info("No changes detected for %s skipping download.", url)
        else:
            log.info("Downloading: %s", url)
            r = services.http.get(url)
            log.info(r.status_code)
            dst.write_bytes(r.content)
        print(dst)
    log.info("Done.")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--debug", action="store_true")
    parser.add_argument("-y", "--year", type=int)
    parser.add_argument("archives", type=str, nargs="+")
    args = parser.parse_args()
    run(*args.archives, year=args.year, debug=args.debug)
    return 0


if __name__ == "__main__":
    main()
