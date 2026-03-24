import pandas as pd
from sqlalchemy import create_engine
from config import DB_URL

engine = create_engine(DB_URL)


def get_live_matches() -> pd.DataFrame:
    """Fetch current and recent matches from DB."""
    return pd.read_sql("""
        SELECT
            match_id, name, status,
            team1, team2, winner,
            venue, match_date, toss_winner
        FROM matches
        ORDER BY match_date DESC
        LIMIT 10
    """, engine)


def get_match_innings(match_id: str) -> pd.DataFrame:
    """Fetch innings scores for a specific match."""
    return pd.read_sql(f"""
        SELECT inning_name, runs, wickets, overs
        FROM innings
        WHERE match_id = '{match_id}'
        ORDER BY id ASC
    """, engine)


def get_match_batting(match_id: str) -> pd.DataFrame:
    """Fetch batting scorecard for a specific match."""
    return pd.read_sql(f"""
        SELECT
            player_name, team, runs,
            balls_faced, fours, sixes, strike_rate
        FROM player_stats
        WHERE match_id = '{match_id}'
          AND role = 'batting'
          AND balls_faced > 0
        ORDER BY runs DESC
    """, engine)


def get_match_bowling(match_id: str) -> pd.DataFrame:
    """Fetch bowling scorecard for a specific match."""
    return pd.read_sql(f"""
        SELECT
            player_name, team, wickets,
            overs_bowled, economy
        FROM player_stats
        WHERE match_id = '{match_id}'
          AND role = 'bowling'
          AND overs_bowled > 0
        ORDER BY wickets DESC, economy ASC
    """, engine)


def get_top_batters(team1: str, team2: str) -> pd.DataFrame:
    """Fetch rolling batting stats for both teams."""
    return pd.read_sql(f"""
        SELECT
            ps.player_name,
            ps.team,
            ROUND(AVG(ps.runs)::numeric, 1)         AS avg_runs,
            ROUND(AVG(ps.strike_rate)::numeric, 1)  AS avg_sr,
            SUM(ps.fours)                            AS total_fours,
            SUM(ps.sixes)                            AS total_sixes,
            COUNT(*)                                 AS innings
        FROM player_stats ps
        JOIN matches m ON ps.match_id = m.match_id
        WHERE ps.role = 'batting'
          AND ps.balls_faced > 3
          AND ps.team IN ('{team1}', '{team2}')
        GROUP BY ps.player_name, ps.team
        HAVING COUNT(*) >= 2
        ORDER BY AVG(ps.runs) DESC
        LIMIT 8
    """, engine)


def get_top_bowlers(team1: str, team2: str) -> pd.DataFrame:
    """Fetch rolling bowling stats for both teams."""
    return pd.read_sql(f"""
        SELECT
            ps.player_name,
            ps.team,
            ROUND(AVG(ps.wickets)::numeric, 1)  AS avg_wickets,
            ROUND(AVG(ps.economy)::numeric, 2)  AS avg_economy,
            SUM(ps.wickets)                      AS total_wickets,
            COUNT(*)                             AS matches
        FROM player_stats ps
        JOIN matches m ON ps.match_id = m.match_id
        WHERE ps.role = 'bowling'
          AND ps.overs_bowled > 0
          AND ps.team IN ('{team1}', '{team2}')
        GROUP BY ps.player_name, ps.team
        HAVING COUNT(*) >= 2
        ORDER BY AVG(ps.wickets) DESC, AVG(ps.economy) ASC
        LIMIT 8
    """, engine)


def get_h2h_summary(team1: str, team2: str) -> dict:
    """Quick H2H summary between two teams."""
    result = pd.read_sql(f"""
        SELECT
            COUNT(*)                                        AS total,
            SUM(CASE WHEN winner = '{team1}' THEN 1 END)   AS team1_wins,
            SUM(CASE WHEN winner = '{team2}' THEN 1 END)   AS team2_wins
        FROM matches
        WHERE winner IS NOT NULL
          AND winner != ''
          AND (
              (team1 = '{team1}' AND team2 = '{team2}') OR
              (team1 = '{team2}' AND team2 = '{team1}')
          )
    """, engine)

    row = result.iloc[0]
    total      = int(row["total"]) if row["total"] else 0
    team1_wins = int(row["team1_wins"]) if row["team1_wins"] else 0
    team2_wins = int(row["team2_wins"]) if row["team2_wins"] else 0

    return {
        "total":      total,
        "team1_wins": team1_wins,
        "team2_wins": team2_wins,
        "team1_wr":   round(team1_wins / total * 100, 1) if total > 0 else 50.0,
        "team2_wr":   round(team2_wins / total * 100, 1) if total > 0 else 50.0,
    }