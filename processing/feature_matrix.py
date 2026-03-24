import pandas as pd
from sqlalchemy import create_engine
from config import DB_URL
from processing.team_features import get_team_form_at_date
from processing.player_features import get_player_features_at_date
from processing.h2h_features import get_h2h_at_date, get_venue_stats, get_toss_impact

engine = create_engine(DB_URL)


def build_feature_matrix() -> pd.DataFrame:
    """
    Build one row per completed match with all features.
    Uses _at_date variants to avoid data leakage —
    features for match on date D only use data from before D.
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
        print("No completed matches to build features from.")
        return pd.DataFrame()

    # Precompute venue and toss stats (these use all historical data)
    venue_stats = get_venue_stats()
    toss_stats  = get_toss_impact()

    rows = []

    for _, match in matches.iterrows():
        date     = str(match["match_date"])
        team1    = match["team1"]
        team2    = match["team2"]
        match_id = match["match_id"]

        # ── Team form (only matches before this date) ──────────────
        form = get_team_form_at_date(date)

        team1_form = _get_team_row(form, team1)
        team2_form = _get_team_row(form, team2)

        # ── H2H stats (only matches before this date) ───────────────
        h2h = get_h2h_at_date(team1, team2, date)

        # ── Venue advantage ─────────────────────────────────────────
        team1_venue_wr = _get_venue_win_rate(venue_stats, team1, match["venue"])
        team2_venue_wr = _get_venue_win_rate(venue_stats, team2, match["venue"])

        # ── Toss impact ──────────────────────────────────────────────
        toss_winner    = match.get("toss_winner", "")
        team1_toss_adv = _get_toss_advantage(toss_stats, team1)
        team2_toss_adv = _get_toss_advantage(toss_stats, team2)
        toss_won_by_team1 = int(toss_winner == team1)

        # ── Player strength proxy ────────────────────────────────────
        player_features = get_player_features_at_date(date)
        team1_bat_avg = _get_team_batting_avg(player_features, team1)
        team2_bat_avg = _get_team_batting_avg(player_features, team2)
        team1_bowl_eco = _get_team_bowling_eco(player_features, team1)
        team2_bowl_eco = _get_team_bowling_eco(player_features, team2)

        # ── Target variable ──────────────────────────────────────────
        team1_won = int(match["winner"] == team1)

        row = {
            "match_id":             match_id,
            "match_date":           date,
            "team1":                team1,
            "team2":                team2,

            # Team form
            "team1_win_rate":       team1_form.get("rolling_win_rate", 0.5),
            "team2_win_rate":       team2_form.get("rolling_win_rate", 0.5),
            "team1_streak":         team1_form.get("streak", 0),
            "team2_streak":         team2_form.get("streak", 0),
            "win_rate_diff":        round(
                team1_form.get("rolling_win_rate", 0.5) -
                team2_form.get("rolling_win_rate", 0.5), 3
            ),

            # H2H
            "h2h_total":            h2h["h2h_total"],
            "team1_h2h_win_rate":   h2h["team_a_win_rate"],
            "team2_h2h_win_rate":   h2h["team_b_win_rate"],

            # Venue
            "team1_venue_win_rate": team1_venue_wr,
            "team2_venue_win_rate": team2_venue_wr,
            "venue_advantage_diff": round(team1_venue_wr - team2_venue_wr, 3),

            # Toss
            "toss_won_by_team1":    toss_won_by_team1,
            "team1_toss_advantage": team1_toss_adv,
            "team2_toss_advantage": team2_toss_adv,

            # Player strength
            "team1_bat_avg":        team1_bat_avg,
            "team2_bat_avg":        team2_bat_avg,
            "bat_avg_diff":         round(team1_bat_avg - team2_bat_avg, 2),
            "team1_bowl_eco":       team1_bowl_eco,
            "team2_bowl_eco":       team2_bowl_eco,
            "bowl_eco_diff":        round(team1_bowl_eco - team2_bowl_eco, 2),

            # Target
            "team1_won":            team1_won,
        }

        rows.append(row)
        print(f"  Built features for: {team1} vs {team2} on {date}")

    matrix = pd.DataFrame(rows)
    print(f"\nFeature matrix shape: {matrix.shape}")
    return matrix


def save_feature_matrix(matrix: pd.DataFrame):
    """Save feature matrix to PostgreSQL for use in Block 3."""
    matrix.to_sql(
        "feature_matrix",
        engine,
        if_exists="replace",
        index=False
    )
    print("Feature matrix saved to PostgreSQL table: feature_matrix")


# ── Helper functions ────────────────────────────────────────────────────────

def _get_team_row(form_df: pd.DataFrame, team: str) -> dict:
    if form_df.empty:
        return {}
    row = form_df[form_df["team"] == team]
    return row.iloc[0].to_dict() if not row.empty else {}


def _get_venue_win_rate(venue_df: pd.DataFrame, team: str, venue: str) -> float:
    if venue_df.empty:
        return 0.5
    row = venue_df[
        (venue_df["team"] == team) &
        (venue_df["venue"] == venue)
    ]
    return float(row["venue_win_rate"].iloc[0]) if not row.empty else 0.5


def _get_toss_advantage(toss_df: pd.DataFrame, team: str) -> float:
    if toss_df.empty:
        return 0.0
    row = toss_df[toss_df["team"] == team]
    return float(row["toss_advantage"].iloc[0]) if not row.empty else 0.0


def _get_team_batting_avg(player_df: pd.DataFrame, team: str) -> float:
    """Average rolling batting average across all players in the team."""
    if player_df.empty:
        return 0.0
    team_players = player_df[player_df["team"] == team]
    if team_players.empty:
        return 0.0
    return round(float(team_players["rolling_avg_runs"].mean()), 2)


def _get_team_bowling_eco(player_df: pd.DataFrame, team: str) -> float:
    """Average rolling bowling economy across all bowlers in the team."""
    if player_df.empty:
        return 0.0
    team_players = player_df[
        (player_df["team"] == team) &
        (player_df["rolling_avg_economy"] > 0)
    ]
    if team_players.empty:
        return 0.0
    return round(float(team_players["rolling_avg_economy"].mean()), 2)

if __name__ == "__main__":
    print("Building feature matrix...")

    matrix = build_feature_matrix()

    if not matrix.empty:
        save_feature_matrix(matrix)
    else:
        print("Feature matrix is empty. Nothing to save.")