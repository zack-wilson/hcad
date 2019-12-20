import os
from datetime import datetime as dt
from pathlib import Path

DEBUG = False
DOMAIN = "https://pdata.hcad.org"
DATABASE = Path("data/hcad").absolute()
LANDING = DATABASE.joinpath("landing")
STAGING = DATABASE.joinpath("staging")
RELEASE = DATABASE.joinpath("release")
