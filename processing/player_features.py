import pandas as pd
from sqlalchemy import create_engine
from config import DB_URL

engine = create_engine(DB_URL)


def get_batting_features() -> pd.DataFrame:
    """
    For each batsman compute:
    - rolling avg runs over last 5 innings
    - rolling avg strike rate over last 5 innings
    - strike rate trend (improving or declining)
    - total innings played
    """

    batting = pd.read_sql("""
        SELECT
            ps.player_name,
            ps.team,
            ps.runs,
            ps.balls_faced,
            ps.strike_rate,
            ps.fours,
            ps.sixes,
            m.match_date
        FROM player_stats ps
        JOIN matches m ON ps.match_id = m.match_id
        WHERE ps.role = 'batting'
          AND ps.balls_faced > 0
        ORDER BY ps.player_name, m.match_date ASC
    """, engine)

    if batting.empty:
        print("No batting data found.")
        return pd.DataFrame()

    # Rolling stats per player
    batting["rolling_avg_runs"] = (
        batting.groupby("player_name")["runs"]
        .transform(lambda x: x.rolling(5, min_periods=1).mean())
        .round(2)
    )

    batting["rolling_avg_sr"] = (
        batting.groupby("player_name")["strike_rate"]
        .transform(lambda x: x.rolling(5, min_periods=1).mean())
        .round(2)
    )

    # Strike rate trend: difference between last 3 and previous 3 games
    # Positive = improving, Negative = declining
    batting["sr_trend"] = (
        batting.groupby("player_name")["strike_rate"]
        .transform(_compute_trend)
        .round(2)
    )

    # Boundary rate: (4s + 6s) / balls faced
    batting["boundary_rate"] = (
        (batting["fours"] + batting["sixes"]) / batting["balls_faced"]
    ).round(3)

    batting["innings_played"] = (
        batting.groupby("player_name").cumcount() + 1
    )

    # Latest snapshot per player
    latest = (
        batting.sort_values("match_date")
        .groupby("player_name")
        .last()
        .reset_index()
    )[[
        "player_name", "team",
        "rolling_avg_runs", "rolling_avg_sr",
        "sr_trend", "boundary_rate", "innings_played"
    ]]

    return latest


def get_bowling_features() -> pd.DataFrame:
    """
    For each bowler compute:
    - rolling avg wickets over last 5 matches
    - rolling avg economy over last 5 matches
    - economy trend (improving = going down)
    - total matches bowled
    """

    bowling = pd.read_sql("""
        SELECT
            ps.player_name,
            ps.team,
            ps.wickets,
            ps.overs_bowled,
            ps.economy,
            m.match_date
        FROM player_stats ps
        JOIN matches m ON ps.match_id = m.match_id
        WHERE ps.role = 'bowling'
          AND ps.overs_bowled > 0
        ORDER BY ps.player_name, m.match_date ASC
    """, engine)

    if bowling.empty:
        print("No bowling data found.")
        return pd.DataFrame()

    bowling["rolling_avg_wickets"] = (
        bowling.groupby("player_name")["wickets"]
        .transform(lambda x: x.rolling(5, min_periods=1).mean())
        .round(2)
    )

    bowling["rolling_avg_economy"] = (
        bowling.groupby("player_name")["economy"]
        .transform(lambda x: x.rolling(5, min_periods=1).mean())
        .round(2)
    )

    # Economy trend: negative = improving (economy going down is good)
    bowling["economy_trend"] = (
        bowling.groupby("player_name")["economy"]
        .transform(_compute_trend)
        .round(2)
    )

    bowling["matches_bowled"] = (
        bowling.groupby("player_name").cumcount() + 1
    )

    latest = (
        bowling.sort_values("match_date")
        .groupby("player_name")
        .last()
        .reset_index()
    )[[
        "player_name", "team",
        "rolling_avg_wickets", "rolling_avg_economy",
        "economy_trend", "matches_bowled"
    ]]

    return latest


def get_player_features_at_date(date: str) -> pd.DataFrame:
    """
    Returns combined batting + bowling features for all players
    using only matches before the given date.
    Used to build feature matrix without data leakage.
    """

    batting = pd.read_sql(f"""
        SELECT
            ps.player_name, ps.team, ps.runs,
            ps.balls_faced, ps.strike_rate,
            ps.fours, ps.sixes, m.match_date
        FROM player_stats ps
        JOIN matches m ON ps.match_id = m.match_id
        WHERE ps.role = 'batting'
          AND ps.balls_faced > 0
          AND m.match_date < '{date}'
        ORDER BY ps.player_name, m.match_date ASC
    """, engine)

    bowling = pd.read_sql(f"""
        SELECT
            ps.player_name, ps.team,
            ps.wickets, ps.overs_bowled,
            ps.economy, m.match_date
        FROM player_stats ps
        JOIN matches m ON ps.match_id = m.match_id
        WHERE ps.role = 'bowling'
          AND ps.overs_bowled > 0
          AND m.match_date < '{date}'
        ORDER BY ps.player_name, m.match_date ASC
    """, engine)

    results = {}

    if not batting.empty:
        batting["rolling_avg_runs"] = (
            batting.groupby("player_name")["runs"]
            .transform(lambda x: x.rolling(5, min_periods=1).mean())
            .round(2)
        )
        batting["rolling_avg_sr"] = (
            batting.groupby("player_name")["strike_rate"]
            .transform(lambda x: x.rolling(5, min_periods=1).mean())
            .round(2)
        )
        batting["sr_trend"] = (
            batting.groupby("player_name")["strike_rate"]
            .transform(_compute_trend)
            .round(2)
        )
        bat_latest = (
            batting.sort_values("match_date")
            .groupby("player_name").last().reset_index()
        )[["player_name", "team", "rolling_avg_runs",
           "rolling_avg_sr", "sr_trend"]]
        results["batting"] = bat_latest

    if not bowling.empty:
        bowling["rolling_avg_wickets"] = (
            bowling.groupby("player_name")["wickets"]
            .transform(lambda x: x.rolling(5, min_periods=1).mean())
            .round(2)
        )
        bowling["rolling_avg_economy"] = (
            bowling.groupby("player_name")["economy"]
            .transform(lambda x: x.rolling(5, min_periods=1).mean())
            .round(2)
        )
        bowling["economy_trend"] = (
            bowling.groupby("player_name")["economy"]
            .transform(_compute_trend)
            .round(2)
        )
        bowl_latest = (
            bowling.sort_values("match_date")
            .groupby("player_name").last().reset_index()
        )[["player_name", "team", "rolling_avg_wickets",
           "rolling_avg_economy", "economy_trend"]]
        results["bowling"] = bowl_latest

    if not results:
        return pd.DataFrame()

    # Merge batting and bowling on player_name
    if "batting" in results and "bowling" in results:
        combined = pd.merge(
            results["batting"], results["bowling"],
            on=["player_name", "team"], how="outer"
        ).fillna(0)
    elif "batting" in results:
        combined = results["batting"]
    else:
        combined = results["bowling"]

    return combined


def _compute_trend(series: pd.Series) -> pd.Series:
    """
    Trend = mean of last 3 values minus mean of previous 3 values.
    Positive = metric going up, Negative = metric going down.
    Min 3 values needed, else returns 0.
    """
    trends = []
    vals = list(series)
    for i in range(len(vals)):
        if i < 3:
            trends.append(0.0)
        else:
            recent = sum(vals[max(0, i-3):i]) / 3
            older  = sum(vals[max(0, i-6):max(0, i-3)]) / max(1, min(3, i-3))
            trends.append(round(recent - older, 2))
    return pd.Series(trends, index=series.index)

