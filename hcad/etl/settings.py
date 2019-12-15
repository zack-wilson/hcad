import os
from datetime import datetime as dt
from pathlib import Path

DEBUG = False
TAX_YEAR = os.getenv("YEAR", dt.now().strftime("%Y"))
DICTIONARY = "Desc/Layout_and_Length.txt"
DOMAIN = "https://pdata.hcad.org"
REMOTE = "data/cama/{year}"
LANDING = Path(f"data/landing/").absolute()
STAGING = Path(f"data/staging/").absolute()
RELEASE = Path(f"data/release/").absolute()
