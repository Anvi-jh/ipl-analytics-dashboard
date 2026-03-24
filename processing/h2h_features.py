import pandas as pd
from sqlalchemy import create_engine
from config import DB_URL

engine = create_engine(DB_URL)


def get_h2h_stats() -> pd.DataFrame:
    """
    For every team pair compute:
    - total matches played between them
    - win rate for team1 vs team2
    - venue advantage (win rate at each ground)
    - toss impact (win rate when winning toss)
    """

    matches = pd.read_sql("""
        SELECT
            match_id, match_date, team1, team2,
            winner, venue, toss_winner
        FROM matches
        WHERE winner IS NOT NULL AND winner != ''
        ORDER BY match_date ASC
    """, engine)

    if matches.empty:
        print("No completed matches found.")
        return pd.DataFrame()

    records = []

    # Get all unique team pairs
    teams = pd.concat([matches["team1"], matches["team2"]]).unique()

    for i, team_a in enumerate(teams):
        for team_b in teams[i+1:]:

            # All matches between these two teams (either order)
            h2h = matches[
                ((matches["team1"] == team_a) & (matches["team2"] == team_b)) |
                ((matches["team1"] == team_b) & (matches["team2"] == team_a))
            ].copy()

            if h2h.empty:
                continue

            total = len(h2h)
            team_a_wins = (h2h["winner"] == team_a).sum()
            team_b_wins = (h2h["winner"] == team_b).sum()

            record = {
                "team_a":            team_a,
                "team_b":            team_b,
                "total_matches":     total,
                "team_a_wins":       int(team_a_wins),
                "team_b_wins":       int(team_b_wins),
                "team_a_win_rate":   round(team_a_wins / total, 3),
                "team_b_win_rate":   round(team_b_wins / total, 3),
            }
            records.append(record)

    h2h_df = pd.DataFrame(records)
    return h2h_df


def get_venue_stats() -> pd.DataFrame:
    """
    For each team at each venue compute:
    - matches played at venue
    - win rate at venue
    """

    matches = pd.read_sql("""
        SELECT match_id, match_date, team1, team2, winner, venue
        FROM matches
        WHERE winner IS NOT NULL AND winner != ''
        ORDER BY match_date ASC
    """, engine)

    if matches.empty:
        return pd.DataFrame()

    # Expand to one row per team per match
    team1_view = matches[["match_id", "match_date", "team1", "winner", "venue"]].copy()
    team1_view.rename(columns={"team1": "team"}, inplace=True)
    team1_view["won"] = (team1_view["team"] == team1_view["winner"]).astype(int)

    team2_view = matches[["match_id", "match_date", "team2", "winner", "venue"]].copy()
    team2_view.rename(columns={"team2": "team"}, inplace=True)
    team2_view["won"] = (team2_view["team"] == team2_view["winner"]).astype(int)

    all_teams = pd.concat([team1_view, team2_view], ignore_index=True)

    venue_stats = (
        all_teams.groupby(["team", "venue"])
        .agg(
            matches_at_venue = ("won", "count"),
            wins_at_venue    = ("won", "sum"),
        )
        .reset_index()
    )

    venue_stats["venue_win_rate"] = (
        venue_stats["wins_at_venue"] / venue_stats["matches_at_venue"]
    ).round(3)

    return venue_stats


def get_toss_impact() -> pd.DataFrame:
    """
    For each team compute:
    - win rate when they win the toss
    - win rate when they lose the toss
    - toss advantage score (difference between the two)
    """

    matches = pd.read_sql("""
        SELECT
            match_id, match_date, team1, team2,
            winner, toss_winner
        FROM matches
        WHERE winner IS NOT NULL
          AND winner != ''
          AND toss_winner IS NOT NULL
          AND toss_winner != ''
        ORDER BY match_date ASC
    """, engine)

    if matches.empty:
        return pd.DataFrame()

    # Expand to one row per team
    team1_view = matches[["match_id", "team1", "winner", "toss_winner"]].copy()
    team1_view.rename(columns={"team1": "team"}, inplace=True)

    team2_view = matches[["match_id", "team2", "winner", "toss_winner"]].copy()
    team2_view.rename(columns={"team2": "team"}, inplace=True)

    all_teams = pd.concat([team1_view, team2_view], ignore_index=True)
    all_teams["won_match"] = (all_teams["team"] == all_teams["winner"]).astype(int)
    all_teams["won_toss"]  = (all_teams["team"] == all_teams["toss_winner"]).astype(int)

    # Win rate when toss is won
    won_toss = (
        all_teams[all_teams["won_toss"] == 1]
        .groupby("team")["won_match"]
        .agg(toss_win_matches="count", toss_win_rate="mean")
        .reset_index()
    )
    won_toss["toss_win_rate"] = won_toss["toss_win_rate"].round(3)

    # Win rate when toss is lost
    lost_toss = (
        all_teams[all_teams["won_toss"] == 0]
        .groupby("team")["won_match"]
        .agg(toss_loss_matches="count", toss_loss_rate="mean")
        .reset_index()
    )
    lost_toss["toss_loss_rate"] = lost_toss["toss_loss_rate"].round(3)

    toss_df = pd.merge(won_toss, lost_toss, on="team", how="outer").fillna(0)

    # Toss advantage: positive = team performs better when winning toss
    toss_df["toss_advantage"] = (
        toss_df["toss_win_rate"] - toss_df["toss_loss_rate"]
    ).round(3)

    return toss_df[["team", "toss_win_rate", "toss_loss_rate", "toss_advantage"]]


def get_h2h_at_date(team_a: str, team_b: str, date: str) -> dict:
    """
    Returns h2h stats between two specific teams
    using only matches before given date.
    Used in feature matrix to avoid data leakage.
    """

    matches = pd.read_sql(f"""
        SELECT match_id, team1, team2, winner, venue, toss_winner
        FROM matches
        WHERE winner IS NOT NULL
          AND winner != ''
          AND match_date < '{date}'
          AND (
              (team1 = '{team_a}' AND team2 = '{team_b}') OR
              (team1 = '{team_b}' AND team2 = '{team_a}')
          )
    """, engine)

    if matches.empty:
        return {
            "h2h_total":        0,
            "team_a_win_rate":  0.5,
            "team_b_win_rate":  0.5,
        }

    total        = len(matches)
    team_a_wins  = (matches["winner"] == team_a).sum()

    return {
        "h2h_total":       total,
        "team_a_win_rate": round(team_a_wins / total, 3),
        "team_b_win_rate": round(1 - team_a_wins / total, 3),
    }

