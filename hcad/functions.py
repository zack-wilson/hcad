import re
from typing import List

import requests

from . import settings

if not settings.cache.get("DICTIONARY"):
    settings.cache["DICTIONARY"] = requests.get(
        "https://pdata.hcad.org/Desc/Layout_and_Length.txt"
    )

dictionary = re.findall(
    r"(\w+)\s+(\w+)\s+(\d+)\s?", settings.cache["DICTIONARY"].text
)


def get_fields(table: str) -> List[str]:
    return [i[1] for i in dictionary if i[0] in table]


def get_max_columns(table: str) -> int:
    return len([i for i in dictionary if i[0] in table])


def get_field_size_limit(table: str) -> int:
    return max(int(i[-1]) for i in dictionary if i[0] in table)
