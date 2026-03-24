import pandas as pd
from sqlalchemy import create_engine
from config import DB_URL

engine = create_engine(DB_URL)


def get_team_form() -> pd.DataFrame:
    """
    For each team, compute:
    - last 5 match results (W/L)
    - current streak (e.g. +3 means 3 wins in a row, -2 means 2 losses)
    - rolling win rate over last 5 games
    - total matches played
    """

    matches = pd.read_sql("""
        SELECT match_id, match_date, team1, team2, winner
        FROM matches
        WHERE winner IS NOT NULL AND winner != ''
        ORDER BY match_date ASC
    """, engine)

    if matches.empty:
        print("No completed matches found.")
        return pd.DataFrame()

    # Convert each match into two rows — one per team
    # so we can compute per-team stats easily
    team1_view = matches[["match_id", "match_date", "team1", "winner"]].copy()
    team1_view.rename(columns={"team1": "team"}, inplace=True)
    team1_view["won"] = (team1_view["team"] == team1_view["winner"]).astype(int)

    team2_view = matches[["match_id", "match_date", "team2", "winner"]].copy()
    team2_view.rename(columns={"team2": "team"}, inplace=True)
    team2_view["won"] = (team2_view["team"] == team2_view["winner"]).astype(int)

    team_matches = pd.concat([team1_view, team2_view], ignore_index=True)
    team_matches.sort_values(["team", "match_date"], inplace=True)
    team_matches.reset_index(drop=True, inplace=True)

    # Rolling win rate over last 5 matches per team
    team_matches["rolling_win_rate"] = (
        team_matches.groupby("team")["won"]
        .transform(lambda x: x.rolling(5, min_periods=1).mean())
        .round(3)
    )

    # Current streak per team
    # +N = N wins in a row, -N = N losses in a row
    team_matches["streak"] = team_matches.groupby("team")["won"].transform(_compute_streak)

    # Total matches played per team
    team_matches["matches_played"] = (
        team_matches.groupby("team").cumcount() + 1
    )

    # Keep only the latest record per team (current form snapshot)
    latest_form = (
        team_matches.sort_values("match_date")
        .groupby("team")
        .last()
        .reset_index()
    )[["team", "rolling_win_rate", "streak", "matches_played"]]

    return latest_form


def get_team_form_at_date(date: str) -> pd.DataFrame:
    """
    Same as get_team_form() but only using matches before a given date.
    Used to build the feature matrix without data leakage.
    """
    matches = pd.read_sql(f"""
        SELECT match_id, match_date, team1, team2, winner
        FROM matches
        WHERE winner IS NOT NULL
          AND winner != ''
          AND match_date < '{date}'
        ORDER BY match_date ASC
    """, engine)

    if matches.empty:
        return pd.DataFrame()

    team1_view = matches[["match_id", "match_date", "team1", "winner"]].copy()
    team1_view.rename(columns={"team1": "team"}, inplace=True)
    team1_view["won"] = (team1_view["team"] == team1_view["winner"]).astype(int)

    team2_view = matches[["match_id", "match_date", "team2", "winner"]].copy()
    team2_view.rename(columns={"team2": "team"}, inplace=True)
    team2_view["won"] = (team2_view["team"] == team2_view["winner"]).astype(int)

    team_matches = pd.concat([team1_view, team2_view], ignore_index=True)
    team_matches.sort_values(["team", "match_date"], inplace=True)

    team_matches["rolling_win_rate"] = (
        team_matches.groupby("team")["won"]
        .transform(lambda x: x.rolling(5, min_periods=1).mean())
        .round(3)
    )
    team_matches["streak"] = (
        team_matches.groupby("team")["won"]
        .transform(_compute_streak)
    )
    team_matches["matches_played"] = (
        team_matches.groupby("team").cumcount() + 1
    )

    latest_form = (
        team_matches.sort_values("match_date")
        .groupby("team")
        .last()
        .reset_index()
    )[["team", "rolling_win_rate", "streak", "matches_played"]]

    return latest_form


def _compute_streak(series: pd.Series) -> pd.Series:
    """
    Compute running streak for a series of 1s and 0s.
    +N for win streak, -N for loss streak.
    """
    streaks = []
    current = 0
    for val in series:
        if val == 1:
            current = current + 1 if current > 0 else 1
        else:
            current = current - 1 if current < 0 else -1
        streaks.append(current)
    return pd.Series(streaks, index=series.index)

