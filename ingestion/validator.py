from ingestion.db_writer import log_error

REQUIRED_MATCH_FIELDS = ["match_id", "name", "team1", "team2", "match_date"]


def validate_match(raw: dict, source: str = "espn") -> dict | None:
    errors = []

    # Check required fields
    for field in REQUIRED_MATCH_FIELDS:
        if not raw.get(field):
            errors.append(f"Missing field: {field}")

    # Date format check
    date_val = str(raw.get("match_date", ""))
    if date_val and len(date_val) < 10:
        errors.append(f"Invalid date: {date_val}")

    # Team names not identical
    if raw.get("team1") == raw.get("team2"):
        errors.append("team1 and team2 are identical")

    if errors:
        log_error(source, raw, "; ".join(errors))
        return None

    return raw


def validate_player_stat(raw: dict, match_id: str, source: str = "espn") -> dict | None:
    errors = []

    if not raw.get("player_name"):
        errors.append("Missing player_name")

    # Coerce numeric fields, default missing to 0
    numeric_fields = {
        "runs": int, "balls_faced": int,
        "fours": int, "sixes": int,
        "wickets": int, "strike_rate": float,
        "overs_bowled": float, "economy": float,
    }
    for field, cast in numeric_fields.items():
        val = raw.get(field)
        try:
            raw[field] = cast(val) if val is not None else 0
            if raw[field] < 0:
                errors.append(f"{field} is negative")
        except (ValueError, TypeError):
            errors.append(f"Cannot coerce {field}: {val}")

    # Sanity checks
    if raw.get("strike_rate", 0) > 500:
        errors.append(f"Implausible strike_rate: {raw['strike_rate']}")
    if raw.get("runs", 0) > 200:
        errors.append(f"Implausible runs in T20: {raw['runs']}")

    if errors:
        log_error(source, {**raw, "match_id": match_id}, "; ".join(errors))
        return None

    return raw