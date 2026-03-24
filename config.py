import os
from dotenv import load_dotenv

load_dotenv()

# ESPN Cricinfo hidden API — no key needed
ESPN_BASE_URL   = "https://site.api.espn.com/apis/site/v2/sports/cricket"
IPL_SERIES_ID   = "8048"
SCOREBOARD_URL  = f"{ESPN_BASE_URL}/{IPL_SERIES_ID}/scoreboard"
MATCH_URL       = f"{ESPN_BASE_URL}/{IPL_SERIES_ID}/summary"
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
GEMINI_MODEL   = "gemini-2.5-flash" # free tier, fast
INSIGHT_CACHE_TTL = 300                  # cache insights for 5 minutes

# PostgreSQL
DB_CONFIG = {
    "host":     os.getenv("PG_HOST", "localhost"),
    "port":     int(os.getenv("PG_PORT", 5432)),
    "dbname":   os.getenv("PG_DB", "ipl_analytics"),
    "user":     os.getenv("PG_USER", "postgres"),
    "password": os.getenv("PG_PASSWORD", ""),
}

DB_URL = (
    f"postgresql://{os.getenv('PG_USER','postgres')}:"
    f"{os.getenv('PG_PASSWORD','')}@"
    f"{os.getenv('PG_HOST','localhost')}:"
    f"{os.getenv('PG_PORT',5432)}/"
    f"{os.getenv('PG_DB','ipl_analytics')}"
)

POLL_INTERVAL_SECONDS = 60