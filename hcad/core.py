import re
from typing import Tuple

from .etl.jobs import land


def archives() -> Tuple[str]:
    ...


def tables() -> Tuple[str]:
    ...


def fields(table) -> Tuple[str]:
    ...


def do_land():
    land.run()
