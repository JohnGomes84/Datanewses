import os
import sys
import time

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..")))

from services.ingestion.src import ingest_bcb, ingest_comex, scrape_news
from shared import config, get_logger

logger = get_logger(__name__)


def run_all():
    logger.info("Starting ingestion cycle")
    ingest_comex.fetch_comex_export()
    ingest_bcb.fetch_bcb_series()
    scrape_news.scrape_economic_news()
    logger.info("Ingestion cycle completed")


if __name__ == "__main__":
    scheduler = BackgroundScheduler()
    scheduler.add_job(run_all, trigger=IntervalTrigger(seconds=config.RUN_INTERVAL))
    scheduler.start()
    logger.info(f"Scheduler started, interval={config.RUN_INTERVAL}s")
    run_all()
    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        scheduler.shutdown()
