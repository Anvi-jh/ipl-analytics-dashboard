import psycopg2
from config import DB_CONFIG


def get_conn():
    return psycopg2.connect(**DB_CONFIG)


def init_db():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS matches (
            match_id        TEXT PRIMARY KEY,
            name            TEXT,
            status          TEXT,
            venue           TEXT,
            match_date      DATE,
            team1           TEXT,
            team2           TEXT,
            winner          TEXT,
            home_away       TEXT,
            source          TEXT,
            fetched_at      TIMESTAMPTZ DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS innings (
            id              SERIAL PRIMARY KEY,
            match_id        TEXT REFERENCES matches(match_id),
            inning_name     TEXT,
            runs            INTEGER,
            wickets         INTEGER,
            overs           NUMERIC(5,1),
            fetched_at      TIMESTAMPTZ DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS player_stats (
            id              SERIAL PRIMARY KEY,
            match_id        TEXT REFERENCES matches(match_id),
            player_name     TEXT,
            team            TEXT,
            role            TEXT,
            runs            INTEGER,
            balls_faced     INTEGER,
            fours           INTEGER,
            sixes           INTEGER,
            strike_rate     NUMERIC(6,2),
            wickets         INTEGER,
            overs_bowled    NUMERIC(5,1),
            economy         NUMERIC(5,2),
            fetched_at      TIMESTAMPTZ DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS data_errors (
            id              SERIAL PRIMARY KEY,
            source          TEXT,
            raw_data        TEXT,
            error_message   TEXT,
            logged_at       TIMESTAMPTZ DEFAULT NOW()
        );

        CREATE INDEX IF NOT EXISTS idx_matches_date  ON matches(match_date);
        CREATE INDEX IF NOT EXISTS idx_matches_teams ON matches(team1, team2);
        CREATE INDEX IF NOT EXISTS idx_player_name   ON player_stats(player_name);
    """)

    conn.commit()
    cur.close()
    conn.close()
    print("All tables created successfully.")


def log_error(source: str, raw_data: str, error_message: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO data_errors (source, raw_data, error_message)
        VALUES (%s, %s, %s)
    """, (source, str(raw_data), error_message))
    conn.commit()    # ← was missing
    cur.close()      # ← was missing
    conn.close()     # ← was missing
    
def upsert_match(match: dict, source: str = "espn"):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO matches (
            match_id, name, status, venue, match_date,
            team1, team2, winner, source
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (match_id) DO UPDATE SET
            status     = EXCLUDED.status,
            winner     = EXCLUDED.winner,
            fetched_at = NOW()
    """, (
        match["match_id"], match["name"], match["status"],
        match["venue"], match["match_date"],
        match["team1"], match["team2"],
        match.get("winner", ""), source
    ))
    conn.commit()
    cur.close()
    conn.close()


def insert_innings(match_id: str, inning_name: str, runs: int, wickets: int, overs: float):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO innings (match_id, inning_name, runs, wickets, overs)
        VALUES (%s, %s, %s, %s, %s)
    """, (match_id, inning_name, runs, wickets, overs))
    conn.commit()
    cur.close()
    conn.close()


def insert_player_stat(match_id: str, stat: dict):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO player_stats (
            match_id, player_name, team, role,
            runs, balls_faced, fours, sixes, strike_rate,
            wickets, overs_bowled, economy
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        match_id,
        stat["player_name"], stat.get("team", ""), stat.get("role", ""),
        stat["runs"], stat["balls_faced"],
        stat["fours"], stat["sixes"], stat["strike_rate"],
        stat["wickets"], stat["overs_bowled"], stat["economy"],
    ))
    conn.commit()
    cur.close()
    conn.close()