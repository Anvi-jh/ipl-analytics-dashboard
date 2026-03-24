# 🏏 IPL Analytics Dashboard

A full-stack data science project combining a live data pipeline, machine learning, and LLM-powered insights into a real-time IPL cricket analytics dashboard.

## What it does
- Polls ESPN Cricinfo every 60 seconds for live IPL match data
- Stores match, innings, and player stats in PostgreSQL
- Engineers 20 features per match — team form, H2H, venue advantage, toss impact
- Predicts win probability using a tuned XGBoost classifier
- Generates pre-match, live, and post-match analyst commentary via Gemini 2.5 Flash
- Displays everything in an auto-refreshing Streamlit dashboard

## Tech stack
| Layer | Tools |
|---|---|
| Data pipeline | Python, ESPN Cricinfo API, APScheduler |
| Database | PostgreSQL, SQLAlchemy |
| Feature engineering | pandas, SQL window functions |
| ML model | scikit-learn, XGBoost, GridSearchCV |
| LLM insights | Google Gemini 2.5 Flash, prompt engineering |
| Dashboard | Streamlit, Plotly |

## Project structure
```
ipl-analytics-dashboard/
├── ingestion/       # ESPN API poller + validator + DB writer
├── processing/      # Feature engineering + match feature matrix
├── models/          # Baseline + XGBoost + live inference
├── llm/             # Prompt templates + Gemini integration + cache
├── dashboard/       # Streamlit app + components + data loader
└── notebooks/       # EDA, model evaluation, verification
```

## Setup
```bash
git clone https://github.com/Anvi-jh/ipl-analytics-dashboard
cd ipl-analytics-dashboard
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
```

Create a `.env` file in the root:
```
PG_HOST=localhost
PG_PORT=5432
PG_DB=ipl_analytics
PG_USER=postgres
PG_PASSWORD=your_password
GEMINI_API_KEY=your_gemini_key
```

## How to run

**Step 1 — Initialize database and start live data pipeline:**
```bash
python main.py
```

**Step 2 — Build feature matrix (run after data is collected):**
```bash
python -c "from processing.feature_matrix import build_feature_matrix, save_feature_matrix; m = build_feature_matrix(); save_feature_matrix(m) if not m.empty else None"
```

**Step 3 — Train models:**
```bash
python models/baseline.py
python models/xgboost_model.py
```

**Step 4 — Launch dashboard:**
```bash
streamlit run dashboard/app.py
```

## Key design decisions
- **Validation layer** — bad rows logged to `data_errors` table, never silently dropped
- **No data leakage** — features computed using only matches before each match date
- **LLM grounding** — Gemini only narrates computed statistics, never invents facts
- **Rate limiter + cache** — insights cached 5 minutes, max 10 Gemini calls/minute
- **Fallback logic** — dashboard never crashes if model or API is unavailable
