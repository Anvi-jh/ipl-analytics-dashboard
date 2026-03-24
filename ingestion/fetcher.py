from ingestion.api_client import get_current_matches, get_match_detail
from ingestion.validator import validate_match, validate_player_stat
from ingestion.db_writer import upsert_match, insert_innings, insert_player_stat


def fetch_and_store():
    print("\nFetching IPL matches from ESPN...")
    matches = get_current_matches()

    if not matches:
        print("No matches found.")
        return

    for raw_match in matches:
        # Validate match
        match = validate_match(raw_match, source="espn")
        if not match:
            print(f"  Skipped invalid match: {raw_match.get('name')}")
            continue

        # Save match
        upsert_match(match)
        match_id = match["match_id"]
        print(f"  Saved: {match['name']} | {match['status']}")

        # Fetch and save scorecard
        detail = get_match_detail(match_id)
        if not detail:
            continue

        _process_scorecard(match_id, detail)

    print("Done.")


def _process_scorecard(match_id: str, detail: dict):
    """Extract innings and player stats from match detail."""
    try:
        competitions = detail.get("gamepackageJSON", {}).get("scoring", [])
        for inning in competitions:
            inning_name = inning.get("displayName", "")
            runs        = inning.get("runs", 0)
            wickets     = inning.get("wickets", 0)
            overs       = float(inning.get("overs", 0))
            insert_innings(match_id, inning_name, runs, wickets, overs)

        # Batting stats
        batters = detail.get("gamepackageJSON", {}).get("batting", [])
        for b in batters:
            raw = {
                "player_name": b.get("athlete", {}).get("displayName", ""),
                "team":        b.get("team", {}).get("displayName", ""),
                "role":        "batting",
                "runs":        b.get("runs", 0),
                "balls_faced": b.get("ballsFaced", 0),
                "fours":       b.get("fours", 0),
                "sixes":       b.get("sixes", 0),
                "strike_rate": b.get("strikeRate", 0.0),
                "wickets":     0,
                "overs_bowled": 0.0,
                "economy":     0.0,
            }
            stat = validate_player_stat(raw, match_id)
            if stat:
                insert_player_stat(match_id, stat)

        # Bowling stats
        bowlers = detail.get("gamepackageJSON", {}).get("bowling", [])
        for b in bowlers:
            raw = {
                "player_name":  b.get("athlete", {}).get("displayName", ""),
                "team":         b.get("team", {}).get("displayName", ""),
                "role":         "bowling",
                "runs":         0,
                "balls_faced":  0,
                "fours":        0,
                "sixes":        0,
                "strike_rate":  0.0,
                "wickets":      b.get("wickets", 0),
                "overs_bowled": float(b.get("overs", 0)),
                "economy":      float(b.get("economy", 0.0)),
            }
            stat = validate_player_stat(raw, match_id)
            if stat:
                insert_player_stat(match_id, stat)

    except Exception as e:
        print(f"  Error processing scorecard for {match_id}: {e}")