"""Fundkit Config."""

# NAV Scheme parsing variables
NAV_URL = "https://portal.amfiindia.com/spages/NAVAll.txt"
SCHEME_TYPE_PREFIXES = (
    "Open Ended Schemes",
    "Close Ended Schemes",
    "Interval Fund",
)
MF_ID_MAP_URL = "https://raw.githubusercontent.com/Anant-mishra1729/FundKit/refs/heads/data/mf_id_map.json"

# --- Historical data vars ---
HISTORICAL_URL = "https://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx"

# Max simulatenous requests
AMFI_CONCURRENCY = 5

# Retries per chunk
MAX_RETRIES = 3

# Backoffs
BASE_BACKOFF = 1.0
