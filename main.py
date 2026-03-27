import os
from sqlalchemy import create_engine, text
from ingestion.db_writer import init_db
from ingestion.kaggle_loader import load_kaggle_matches, load_kaggle_player_stats
from ingestion.scheduler import start_scheduler
from config import DB_URL

engine = create_engine(DB_URL)


def is_table_empty(table_name: str) -> bool:
    """Check if a table is empty."""
    with engine.connect() as conn:
        result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
        count = result.scalar()
        return count == 0


if __name__ == "__main__":
    os.makedirs("data/kaggle", exist_ok=True)

    # Initialize DB tables
    init_db()

    # ✅ Step 1: Load matches safely (always safe due to UPSERT)
    print("\nChecking/loading matches...")
    load_kaggle_matches()

    # ✅ Step 2: Load player stats ONLY if empty (prevents duplicates)
    print("\nChecking player_stats table...")
    if is_table_empty("player_stats"):
        print("player_stats is empty → loading data...")
        load_kaggle_player_stats()
    else:
        print("player_stats already has data → skipping to avoid duplicates")

    # ✅ Step 3: Start live ESPN polling
    print("\nStarting live ESPN scheduler...")
    start_scheduler()