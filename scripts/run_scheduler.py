import os
import sys
import time

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from shared import config, get_logger
from scripts import test_pipeline

logger = get_logger("PipelineScheduler")


def run_cycle():
    logger.info("Starting scheduled pipeline cycle")
    test_pipeline.run_test()
    logger.info("Scheduled pipeline cycle finished")


if __name__ == "__main__":
    scheduler = BackgroundScheduler()
    scheduler.add_job(run_cycle, trigger=IntervalTrigger(seconds=config.RUN_INTERVAL))
    scheduler.start()
    logger.info(f"Scheduler started with interval={config.RUN_INTERVAL}s")
    run_cycle()
    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        scheduler.shutdown()
