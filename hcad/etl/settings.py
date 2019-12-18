import os
from datetime import datetime as dt
from pathlib import Path

DEBUG = False
TAX_YEAR = os.getenv("YEAR", dt.now().strftime("%Y"))
DOMAIN = "https://pdata.hcad.org"
REMOTE = "data/cama/{year}"
DATABASE = Path("data/hcad").absolute()
DICTIONARY = DATABASE.joinpath("Desc").joinpath("Layout_and_Length.txt")
LANDING = DATABASE.joinpath("landing")
STAGING = DATABASE.joinpath("staging")
RELEASE = DATABASE.joinpath("release")
