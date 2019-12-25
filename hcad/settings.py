import shelve
from datetime import datetime as dt
from pathlib import Path

cache = shelve.open(".cache")
db = Path("data/hcad/")
tax_year = dt.now().strftime("%Y")
landing = db.joinpath("landing")
staging = db.joinpath("staging")
log_fmt = "%(asctime)s:%(levelname)s:%(module)s:%(funcName)s: %(message)s"
