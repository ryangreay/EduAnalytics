import os
from pathlib import Path

DATA_DIR = Path(os.getenv("DATA_DIR", "./data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)

# Smarter Balanced research list page for a given year
CAASPP_LIST = "https://caaspp-elpac.ets.org/caaspp/ResearchFileListSB.aspx?lstCounty=00&lstDistrict=00000&lstTestType=B&lstTestYear={year}&ps=true"
