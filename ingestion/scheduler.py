from apscheduler.schedulers.blocking import BlockingScheduler
from ingestion.fetcher import fetch_and_store
from config import POLL_INTERVAL_SECONDS


def start_scheduler():
    scheduler = BlockingScheduler()
    scheduler.add_job(
        fetch_and_store,
        trigger="interval",
        seconds=POLL_INTERVAL_SECONDS,
        id="ipl_poller",
        name="IPL live data poller",
        max_instances=1,
        misfire_grace_time=30,
    )
    print(f"Scheduler started. Polling ESPN every {POLL_INTERVAL_SECONDS}s.")
    print("Press Ctrl+C to stop.\n")
    fetch_and_store()
    scheduler.start()