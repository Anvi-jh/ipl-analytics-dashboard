import os
from ingestion.db_writer import init_db
from ingestion.scheduler import start_scheduler

if __name__ == "__main__":
    os.makedirs("data", exist_ok=True)
    init_db()
    start_scheduler()