import pandas as pd
from sqlalchemy import create_engine
from config import DB_URL
from llm.insight_generator import get_player_spotlight

engine = create_engine(DB_URL)


def get_top_players_for_match(
    team1: str,
    team2: str,
    date: str,
    top_n: int = 3
) -> list[dict]:
    """
    Pull top in-form players from both teams from the database.
    Returns top batters and bowlers by rolling stats.
    """

    # Top batters by rolling avg runs
    batters = pd.read_sql(f"""
        SELECT
            ps.player_name   AS name,
            ps.team,
            AVG(ps.runs)              AS rolling_avg_runs,
            AVG(ps.strike_rate)       AS rolling_avg_sr,
            COUNT(*)                  AS innings
        FROM player_stats ps
        JOIN matches m ON ps.match_id = m.match_id
        WHERE ps.role = 'batting'
          AND ps.balls_faced > 3
          AND ps.team IN ('{team1}', '{team2}')
          AND m.match_date < '{date}'
        GROUP BY ps.player_name, ps.team
        HAVING COUNT(*) >= 3
        ORDER BY AVG(ps.runs) DESC
        LIMIT {top_n}
    """, engine)

    # Top bowlers by rolling avg wickets
    bowlers = pd.read_sql(f"""
        SELECT
            ps.player_name   AS name,
            ps.team,
            AVG(ps.wickets)           AS rolling_avg_wickets,
            AVG(ps.economy)           AS rolling_avg_economy,
            COUNT(*)                  AS matches
        FROM player_stats ps
        JOIN matches m ON ps.match_id = m.match_id
        WHERE ps.role = 'bowling'
          AND ps.overs_bowled > 0
          AND ps.team IN ('{team1}', '{team2}')
          AND m.match_date < '{date}'
        GROUP BY ps.player_name, ps.team
        HAVING COUNT(*) >= 3
        ORDER BY AVG(ps.wickets) DESC, AVG(ps.economy) ASC
        LIMIT {top_n}
    """, engine)

    players = []

    # Format batters
    for _, row in batters.iterrows():
        players.append({
            "name":                 row["name"],
            "team":                 row["team"],
            "rolling_avg_runs":     round(float(row["rolling_avg_runs"]), 1),
            "rolling_avg_sr":       round(float(row["rolling_avg_sr"]), 1),
            "sr_trend":             _compute_sr_trend(row["name"], date),
            "rolling_avg_wickets":  0,
            "rolling_avg_economy":  0,
            "economy_trend":        0,
        })

    # Format bowlers
    for _, row in bowlers.iterrows():
        players.append({
            "name":                 row["name"],
            "team":                 row["team"],
            "rolling_avg_runs":     0,
            "rolling_avg_sr":       0,
            "sr_trend":             0,
            "rolling_avg_wickets":  round(float(row["rolling_avg_wickets"]), 1),
            "rolling_avg_economy":  round(float(row["rolling_avg_economy"]), 1),
            "economy_trend":        _compute_economy_trend(row["name"], date),
        })

    return players


def _compute_sr_trend(player_name: str, date: str) -> float:
    """
    Trend = avg SR in last 3 games minus avg SR in 3 games before that.
    Positive = improving, Negative = declining.
    """
    recent = pd.read_sql(f"""
        SELECT ps.strike_rate
        FROM player_stats ps
        JOIN matches m ON ps.match_id = m.match_id
        WHERE ps.player_name = '{player_name}'
          AND ps.role = 'batting'
          AND ps.balls_faced > 3
          AND m.match_date < '{date}'
        ORDER BY m.match_date DESC
        LIMIT 6
    """, engine)

    if len(recent) < 4:
        return 0.0

    vals = recent["strike_rate"].tolist()
    recent_avg = sum(vals[:3]) / 3
    older_avg  = sum(vals[3:]) / len(vals[3:])
    return round(recent_avg - older_avg, 1)


def _compute_economy_trend(player_name: str, date: str) -> float:
    """
    Trend = avg economy in last 3 games minus avg economy in 3 games before.
    Negative = improving (economy going down is good for bowler).
    """
    recent = pd.read_sql(f"""
        SELECT ps.economy
        FROM player_stats ps
        JOIN matches m ON ps.match_id = m.match_id
        WHERE ps.player_name = '{player_name}'
          AND ps.role = 'bowling'
          AND ps.overs_bowled > 0
          AND m.match_date < '{date}'
        ORDER BY m.match_date DESC
        LIMIT 6
    """, engine)

    if len(recent) < 4:
        return 0.0

    vals = recent["economy"].tolist()
    recent_avg = sum(vals[:3]) / 3
    older_avg  = sum(vals[3:]) / len(vals[3:])
    return round(recent_avg - older_avg, 1)


def generate_player_spotlight(
    team1: str,
    team2: str,
    date: str,
) -> str:
    """
    Main function called by Block 5 dashboard.
    Pulls real player data and generates LLM spotlight.
    """

    match_name = f"{team1} vs {team2}"
    players    = get_top_players_for_match(team1, team2, date)

    if not players:
        return "Player spotlight unavailable — insufficient match data."

    print(f"  Found {len(players)} players for spotlight")
    for p in players:
        print(f"    {p['name']} ({p['team']})")

    return get_player_spotlight(
        match=match_name,
        players=players,
    )