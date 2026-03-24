def pre_match_prompt(
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
    """
    Prompt for pre-match analyst commentary.
    All numbers come from your computed features — LLM only narrates.
    """

    streak1_str = (
        f"{abs(team1_streak)} win streak" if team1_streak > 0
        else f"{abs(team1_streak)} loss streak" if team1_streak < 0
        else "no current streak"
    )
    streak2_str = (
        f"{abs(team2_streak)} win streak" if team2_streak > 0
        else f"{abs(team2_streak)} loss streak" if team2_streak < 0
        else "no current streak"
    )

    return f"""
You are a professional IPL cricket analyst. Write a concise pre-match analysis
in exactly 3 sentences. Use ONLY the statistics provided below — do not add
any facts, player names, or historical events not mentioned here.

Match: {team1} vs {team2}
Venue: {venue}

Statistics:
- Win probability: {team1} {team1_win_prob}% | {team2} {team2_win_prob}%
- Recent form (last 5 games): {team1} win rate {team1_win_rate*100:.0f}% ({streak1_str}) | {team2} win rate {team2_win_rate*100:.0f}% ({streak2_str})
- Head to head: {team1} wins {team1_h2h_win_rate*100:.0f}% of {h2h_total} meetings
- Venue win rate: {team1} {team1_venue_win_rate*100:.0f}% | {team2} {team2_venue_win_rate*100:.0f}% at {venue}
- Toss: won by {toss_winner}

Instructions:
- Sentence 1: current form and momentum of both teams
- Sentence 2: head to head and venue advantage
- Sentence 3: win probability and prediction
- Tone: professional, data-driven, confident
- Do NOT start with "Certainly" or "Sure" or any filler phrase
""".strip()


def live_match_prompt(
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
    """
    Prompt for live in-match commentary update.
    Called every few minutes during a live game.
    """

    run_rate_str = ""
    if required_rate and current_rate:
        run_rate_str = f"- Required run rate: {required_rate:.2f} | Current run rate: {current_rate:.2f}"

    return f"""
You are a live IPL cricket commentator. Write a single punchy sentence
(max 30 words) summarizing the current match situation.
Use ONLY the data below — no invented details.

Match: {team1} vs {team2}
Current innings: {batting_team} — {runs}/{wickets} in {overs} overs
Win probability: {team1} {team1_win_prob}% | {team2} {team2_win_prob}%
{run_rate_str}

Instructions:
- One sentence only, maximum 30 words
- Mention the batting team's score and win probability shift
- Tone: excited but factual
- Do NOT start with "Certainly" or filler phrases
""".strip()


def post_match_prompt(
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
    """
    Prompt for post-match summary and model accuracy check.
    """

    prediction_str = (
        f"Model correctly predicted {winner} would win ({predicted_prob:.0f}% confidence)"
        if predicted_winner == winner
        else f"Model predicted {predicted_winner} ({predicted_prob:.0f}% confidence) — result was an upset"
    )

    return f"""
You are an IPL post-match analyst. Write exactly 3 sentences summarizing
this match. Use ONLY the statistics below.

Match: {team1} vs {team2}
Result: {winner} won by {win_margin}

Key statistics:
- {team1} top scorer: {team1_top_scorer} — {team1_top_scorer_runs} runs
- {team2} top scorer: {team2_top_scorer} — {team2_top_scorer_runs} runs
- Best bowler: {best_bowler} — {best_bowler_wickets} wickets at {best_bowler_economy:.2f} economy
- Prediction: {prediction_str}

Instructions:
- Sentence 1: how the match was won and key batting performance
- Sentence 2: bowling impact and how it shaped the result
- Sentence 3: comment on the model prediction accuracy
- Tone: analytical, post-match wrap-up
- Do NOT start with filler phrases
""".strip()


def player_spotlight_prompt(
    match: str,
    players: list[dict],
) -> str:
    """
    Prompt to identify 2-3 key players to watch based on their form stats.
    players is a list of dicts with name, team, rolling_avg_runs,
    rolling_avg_sr, sr_trend, rolling_avg_wickets, rolling_avg_economy.
    """

    player_lines = []
    for p in players:
        if p.get("rolling_avg_runs", 0) > 0:
            player_lines.append(
                f"- {p['name']} ({p['team']}): "
                f"avg {p['rolling_avg_runs']} runs, "
                f"SR {p['rolling_avg_sr']}, "
                f"SR trend {'+' if p['sr_trend'] > 0 else ''}{p['sr_trend']}"
            )
        else:
            player_lines.append(
                f"- {p['name']} ({p['team']}): "
                f"{p['rolling_avg_wickets']} wickets/game, "
                f"economy {p['rolling_avg_economy']}, "
                f"economy trend {'+' if p['economy_trend'] > 0 else ''}{p['economy_trend']}"
            )

    players_str = "\n".join(player_lines)

    return f"""
You are an IPL fantasy cricket expert. Based ONLY on the statistics below,
identify 2 players to watch in this match and explain why in one sentence each.

Match: {match}

Player statistics (last 5 games):
{players_str}

Instructions:
- Pick exactly 2 players — one batter, one bowler
- One sentence per player explaining their form
- Base your pick entirely on the numbers provided
- Tone: expert fantasy cricket advice
- Do NOT start with filler phrases
""".strip()