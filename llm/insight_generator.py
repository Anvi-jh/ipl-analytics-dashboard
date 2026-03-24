from google import genai
from config import GEMINI_API_KEY, GEMINI_MODEL
from llm.prompts import (
    pre_match_prompt,
    live_match_prompt,
    post_match_prompt,
    player_spotlight_prompt,
)
from llm.cache import insight_cache
from llm.rate_limiter import rate_limiter

client = genai.Client(api_key=GEMINI_API_KEY)


def _call_gemini(prompt: str) -> str:
    """
    Core Gemini API call with cache + rate limiting + error handling.
    """
    # Check cache first
    cached = insight_cache.get(prompt)
    if cached:
        print("  [cache hit] returning cached insight")
        return cached

    # Respect rate limit
    rate_limiter.wait_if_needed()

    try:
        response = client.models.generate_content(
            model=GEMINI_MODEL,
            contents=prompt,
        )
        result = response.text.strip()

        # Store in cache
        insight_cache.set(result, prompt)
        return result

    except Exception as e:
        print(f"Gemini API error: {e}")
        return "Analyst insight unavailable at this time."


def get_pre_match_insight(
    team1: str,
    team2: str,
    venue: str,
    team1_win_prob: float,
    team2_win_prob: float,
    team1_win_rate: float,
    team2_win_rate: float,
    team1_streak: int,
    team2_streak: int,
    team1_h2h_win_rate: float,
    h2h_total: int,
    team1_venue_win_rate: float,
    team2_venue_win_rate: float,
    toss_winner: str,
) -> str:
    prompt = pre_match_prompt(
        team1=team1,
        team2=team2,
        venue=venue,
        team1_win_prob=team1_win_prob,
        team2_win_prob=team2_win_prob,
        team1_win_rate=team1_win_rate,
        team2_win_rate=team2_win_rate,
        team1_streak=team1_streak,
        team2_streak=team2_streak,
        team1_h2h_win_rate=team1_h2h_win_rate,
        h2h_total=h2h_total,
        team1_venue_win_rate=team1_venue_win_rate,
        team2_venue_win_rate=team2_venue_win_rate,
        toss_winner=toss_winner,
    )
    return _call_gemini(prompt)


def get_live_insight(
    team1: str,
    team2: str,
    team1_win_prob: float,
    team2_win_prob: float,
    batting_team: str,
    runs: int,
    wickets: int,
    overs: float,
    required_rate: float = None,
    current_rate: float = None,
) -> str:
    prompt = live_match_prompt(
        team1=team1,
        team2=team2,
        team1_win_prob=team1_win_prob,
        team2_win_prob=team2_win_prob,
        batting_team=batting_team,
        runs=runs,
        wickets=wickets,
        overs=overs,
        required_rate=required_rate,
        current_rate=current_rate,
    )
    return _call_gemini(prompt)


def get_post_match_insight(
    team1: str,
    team2: str,
    winner: str,
    win_margin: str,
    team1_top_scorer: str,
    team1_top_scorer_runs: int,
    team2_top_scorer: str,
    team2_top_scorer_runs: int,
    best_bowler: str,
    best_bowler_wickets: int,
    best_bowler_economy: float,
    predicted_winner: str,
    predicted_prob: float,
) -> str:
    prompt = post_match_prompt(
        team1=team1,
        team2=team2,
        winner=winner,
        win_margin=win_margin,
        team1_top_scorer=team1_top_scorer,
        team1_top_scorer_runs=team1_top_scorer_runs,
        team2_top_scorer=team2_top_scorer,
        team2_top_scorer_runs=team2_top_scorer_runs,
        best_bowler=best_bowler,
        best_bowler_wickets=best_bowler_wickets,
        best_bowler_economy=best_bowler_economy,
        predicted_winner=predicted_winner,
        predicted_prob=predicted_prob,
    )
    return _call_gemini(prompt)


def get_player_spotlight(
    match: str,
    players: list[dict],
) -> str:
    prompt = player_spotlight_prompt(
        match=match,
        players=players,
    )
    return _call_gemini(prompt)