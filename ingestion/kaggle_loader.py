import pandas as pd
import hashlib
from ingestion.validator import validate_match, validate_player_stat
from ingestion.db_writer import upsert_match, insert_player_stat
from config import DB_URL
from sqlalchemy import create_engine

engine = create_engine(DB_URL)

MATCHES_PATH    = "data/kaggle/matches.csv"
DELIVERIES_PATH = "data/kaggle/deliveries.csv"


def _make_match_id(row) -> str:
    """Stable unique ID from match metadata since Kaggle has no UUID."""
    key = f"{row['date']}_{row['team1']}_{row['team2']}_{row['venue']}"
    return hashlib.md5(key.encode()).hexdigest()[:12]


def _detect_batsman_col(df: pd.DataFrame) -> str:
    """Handle both old ('batsman') and new ('batter') column names."""
    if "batter" in df.columns:
        return "batter"
    elif "batsman" in df.columns:
        return "batsman"
    else:
        raise KeyError("Cannot find batsman/batter column in deliveries CSV")


def load_kaggle_matches():
    print("Loading Kaggle historical matches...")
    df = pd.read_csv(MATCHES_PATH)
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")

    df["match_date"] = pd.to_datetime(
        df["date"], errors="coerce"
    ).dt.strftime("%Y-%m-%d")
    df["match_id"] = df.apply(_make_match_id, axis=1)

    success, failed = 0, 0

    for _, row in df.iterrows():
        raw = {
            "match_id":      row["match_id"],
            "name":          f"{row['team1']} vs {row['team2']}",
            "status":        "completed",
            "venue":         str(row.get("venue", "")),
            "match_date":    row["match_date"],
            "team1":         str(row.get("team1", "")),
            "team2":         str(row.get("team2", "")),
            "toss_winner":   str(row.get("toss_winner", "")),
            "toss_decision": str(row.get("toss_decision", "")),
            "winner":        str(row.get("winner", "")),
        }

        validated = validate_match(raw, source="kaggle")
        if validated:
            upsert_match(validated, source="kaggle")
            success += 1
        else:
            failed += 1

    print(f"Matches loaded: {success} success, {failed} failed")
    return success


def load_kaggle_player_stats():
    print("Loading Kaggle player stats from deliveries...")
    df = pd.read_csv(DELIVERIES_PATH)
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")

    # Detect correct column name
    batsman_col = _detect_batsman_col(df)
    print(f"  Using column: '{batsman_col}' for batsman")

    # Build match ID lookup: kaggle integer id → our hash id
    matches_df = pd.read_csv(MATCHES_PATH)
    matches_df.columns = matches_df.columns.str.strip().str.lower().str.replace(" ", "_")
    matches_df["match_date"] = pd.to_datetime(
        matches_df["date"], errors="coerce"
    ).dt.strftime("%Y-%m-%d")
    matches_df["our_match_id"] = matches_df.apply(_make_match_id, axis=1)
    id_map = dict(zip(matches_df["id"], matches_df["our_match_id"]))

    # ── Batting stats ────────────────────────────────────────────────
    batting = (
        df.groupby(["match_id", batsman_col, "batting_team"])
        .agg(
            runs        = ("batsman_runs", "sum"),
            balls_faced = ("ball", "count"),
            fours       = ("batsman_runs", lambda x: (x == 4).sum()),
            sixes       = ("batsman_runs", lambda x: (x == 6).sum()),
        )
        .reset_index()
    )
    batting.rename(columns={batsman_col: "batsman"}, inplace=True)
    batting["strike_rate"]   = (batting["runs"] / batting["balls_faced"] * 100).round(2)
    batting["our_match_id"]  = batting["match_id"].map(id_map)
    batting = batting.dropna(subset=["our_match_id"])

    # ── Bowling stats ────────────────────────────────────────────────
    # Detect dismissal column — varies across dataset versions
    dismissal_col = None
    for col in ["player_dismissed", "dismissal_kind"]:
        if col in df.columns:
            dismissal_col = col
            break

    if dismissal_col:
        bowling = (
            df.groupby(["match_id", "bowler", "batting_team"])
            .agg(
                balls_bowled = ("ball", "count"),
                runs_given   = ("total_runs", "sum"),
                wickets      = (dismissal_col, lambda x: x.notna().sum()),
            )
            .reset_index()
        )
    else:
        # Fallback if no dismissal column found
        bowling = (
            df.groupby(["match_id", "bowler", "batting_team"])
            .agg(
                balls_bowled = ("ball", "count"),
                runs_given   = ("total_runs", "sum"),
            )
            .reset_index()
        )
        bowling["wickets"] = 0

    bowling["overs_bowled"] = (bowling["balls_bowled"] / 6).round(1)
    bowling["economy"]      = (
        bowling["runs_given"] / bowling["overs_bowled"].replace(0, 1)
    ).round(2)
    bowling["our_match_id"] = bowling["match_id"].map(id_map)
    bowling = bowling.dropna(subset=["our_match_id"])

    success, failed = 0, 0

    # Insert batting
    print(f"  Inserting {len(batting)} batting rows...")
    for _, row in batting.iterrows():
        raw = {
            "player_name":  row["batsman"],
            "team":         row["batting_team"],
            "role":         "batting",
            "runs":         int(row["runs"]),
            "balls_faced":  int(row["balls_faced"]),
            "fours":        int(row["fours"]),
            "sixes":        int(row["sixes"]),
            "strike_rate":  float(row["strike_rate"]),
            "wickets":      0,
            "overs_bowled": 0.0,
            "economy":      0.0,
        }
        validated = validate_player_stat(
            raw, match_id=row["our_match_id"], source="kaggle"
        )
        if validated:
            insert_player_stat(row["our_match_id"], validated)
            success += 1
        else:
            failed += 1

    # Insert bowling
    print(f"  Inserting {len(bowling)} bowling rows...")
    for _, row in bowling.iterrows():
        raw = {
            "player_name":  row["bowler"],
            "team":         row["batting_team"],
            "role":         "bowling",
            "runs":         0,
            "balls_faced":  0,
            "fours":        0,
            "sixes":        0,
            "strike_rate":  0.0,
            "wickets":      int(row["wickets"]),
            "overs_bowled": float(row["overs_bowled"]),
            "economy":      float(row["economy"]),
        }
        validated = validate_player_stat(
            raw, match_id=row["our_match_id"], source="kaggle"
        )
        if validated:
            insert_player_stat(row["our_match_id"], validated)
            success += 1
        else:
            failed += 1

    print(f"Player stats loaded: {success} success, {failed} failed")
    return success