import psycopg2
from config import DB_CONFIG

conn = psycopg2.connect(**DB_CONFIG)
cur  = conn.cursor()

print("=== Database Reset ===\n")

# Step 1: Drop all tables including feature_matrix from Block 2
print("Dropping all tables...")
cur.execute("""
    DROP TABLE IF EXISTS feature_matrix  CASCADE;
    DROP TABLE IF EXISTS player_stats    CASCADE;
    DROP TABLE IF EXISTS innings         CASCADE;
    DROP TABLE IF EXISTS data_errors     CASCADE;
    DROP TABLE IF EXISTS matches         CASCADE;
""")
conn.commit()
print("All tables dropped.")

# Step 2: Drop all indexes (they drop with tables but just to be safe)
cur.execute("""
    DROP INDEX IF EXISTS idx_matches_date;
    DROP INDEX IF EXISTS idx_matches_teams;
    DROP INDEX IF EXISTS idx_player_name;
""")
conn.commit()
print("All indexes dropped.")

conn.close()
print("\nDatabase is now completely clean.")
print("Run python main.py to rebuild from scratch with fresh ESPN data.")