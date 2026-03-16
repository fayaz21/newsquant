from __future__ import annotations

import logging
from pathlib import Path

import yaml
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from scraper.models.source import SourceConfig
from scraper.pipeline.orchestrator import Orchestrator

logger = logging.getLogger(__name__)

_scheduler: BackgroundScheduler | None = None


def load_sources(path: str | None = None) -> list[SourceConfig]:
    if path is None:
        path = str(Path(__file__).parents[2] / "config" / "sources.yaml")
    with open(path) as f:
        data = yaml.safe_load(f)
    return [SourceConfig(**src) for src in data.get("sources", [])]


def _make_job(config: SourceConfig):
    orchestrator = Orchestrator()

    def job():
        logger.info("Scheduler triggering: %s", config.name)
        try:
            orchestrator.run(config, run_type="realtime")
        except Exception as exc:
            logger.error("Scheduled job %s failed: %s", config.name, exc)

    job.__name__ = f"job_{config.name}"
    return job


def start_scheduler(sources_path: str | None = None) -> BackgroundScheduler:
    global _scheduler
    sources = load_sources(sources_path)

    scheduler = BackgroundScheduler(daemon=True)
    registered = 0

    for config in sources:
        if not config.enabled or config.backfill_only or not config.schedule_cron:
            continue
        try:
            trigger = CronTrigger.from_crontab(config.schedule_cron)
            scheduler.add_job(
                _make_job(config),
                trigger=trigger,
                id=f"job_{config.name}",
                name=config.name,
                max_instances=1,
                replace_existing=True,
            )
            registered += 1
            logger.info("Registered scheduler job: %s [%s]", config.name, config.schedule_cron)
        except Exception as exc:
            logger.warning("Failed to register job %s: %s", config.name, exc)

    scheduler.start()
    _scheduler = scheduler
    logger.info("Scheduler started with %d jobs", registered)
    return scheduler


def stop_scheduler() -> None:
    global _scheduler
    if _scheduler and _scheduler.running:
        _scheduler.shutdown(wait=False)
        _scheduler = None
        logger.info("Scheduler stopped")
