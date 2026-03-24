import requests
from config import SCOREBOARD_URL, MATCH_URL


def get_current_matches():
    """Fetch all current/recent IPL matches from ESPN."""
    try:
        response = requests.get(SCOREBOARD_URL, timeout=10)
        response.raise_for_status()
        data = response.json()

        # ESPN returns matches inside events key
        events = data.get("events", [])
        if not events:
            print("No matches found in ESPN response.")
            return []

        matches = []
        for event in events:
            match = {
                "match_id":   event.get("id"),
                "name":       event.get("name"),
                "status":     event.get("status", {}).get("type", {}).get("description", ""),
                "venue":      event.get("competitions", [{}])[0].get("venue", {}).get("fullName", ""),
                "match_date": event.get("date", "")[:10],
                "team1":      event.get("competitions", [{}])[0].get("competitors", [{}])[0].get("team", {}).get("displayName", ""),
                "team2":      event.get("competitions", [{}])[0].get("competitors", [{}])[1].get("team", {}).get("displayName", ""),
                "winner":     _get_winner(event),
            }
            matches.append(match)

        return matches

    except requests.exceptions.RequestException as e:
        print(f"ESPN API error: {e}")
        return []


def get_match_detail(match_id: str):
    """Fetch full scorecard for a specific match."""
    try:
        url = f"{MATCH_URL}?event={match_id}"
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching match {match_id}: {e}")
        return None


def _get_winner(event: dict) -> str:
    """Extract winner name from ESPN event dict."""
    try:
        competitors = event["competitions"][0]["competitors"]
        for c in competitors:
            if c.get("winner"):
                return c["team"]["displayName"]
    except (KeyError, IndexError):
        pass
    return ""