import psycopg2
from config import DB_CONFIG

conn = psycopg2.connect(**DB_CONFIG)
cur = conn.cursor()

print("=" * 45)
print("  IPL ANALYTICS — DATABASE VERIFICATION")
print("=" * 45)

# Matches
cur.execute("SELECT COUNT(*) FROM matches")
print(f"\n Matches stored     : {cur.fetchone()[0]}")

# Innings
cur.execute("SELECT COUNT(*) FROM innings")
print(f" Innings stored     : {cur.fetchone()[0]}")

# Player stats
cur.execute("SELECT COUNT(*) FROM player_stats")
print(f" Player stat rows   : {cur.fetchone()[0]}")

# Data errors caught
cur.execute("SELECT COUNT(*) FROM data_errors")
print(f" Validation errors  : {cur.fetchone()[0]}")

# Latest 3 matches
print("\n Latest matches:")
cur.execute("""
    SELECT name, status, fetched_at
    FROM matches
    ORDER BY fetched_at DESC
    LIMIT 3
""")
for row in cur.fetchall():
    print(f"   {row[0]} | {row[1]} | {row[2]}")

# Top 5 players by runs
print("\n Top 5 players by runs:")
cur.execute("""
    SELECT player_name, team, SUM(runs) as total_runs
    FROM player_stats
    WHERE role = 'batting'
    GROUP BY player_name, team
    ORDER BY total_runs DESC
    LIMIT 5
""")
for row in cur.fetchall():
    print(f"   {row[0]} ({row[1]}) — {row[2]} runs")

# Any validation errors logged
print("\n Recent validation errors (if any):")
cur.execute("""
    SELECT source, error_message, logged_at
    FROM data_errors
    ORDER BY logged_at DESC
    LIMIT 3
""")
rows = cur.fetchall()
if rows:
    for row in rows:
        print(f"   [{row[0]}] {row[1]} at {row[2]}")
else:
    print("   No errors logged — clean data!")

print("\n" + "=" * 45)
print("  Block 1 complete!")
print("=" * 45)

cur.close()
conn.close()