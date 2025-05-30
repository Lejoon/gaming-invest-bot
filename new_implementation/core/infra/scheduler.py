"""
Scheduler infrastructure for running periodic tasks.
"""

import asyncio
import logging
from typing import Callable, Dict, Any, Optional

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger


logger = logging.getLogger(__name__)


class Scheduler:
    """Async task scheduler wrapper around APScheduler."""
    
    def __init__(self):
        self._scheduler = AsyncIOScheduler()
        self._started = False

    async def start(self) -> None:
        """Start the scheduler."""
        if not self._started:
            self._scheduler.start()
            self._started = True
            logger.info("Scheduler started")

    async def stop(self) -> None:
        """Stop the scheduler."""
        if self._started:
            self._scheduler.shutdown()
            self._started = False
            logger.info("Scheduler stopped")

    def add_interval_job(
        self,
        func: Callable,
        seconds: Optional[int] = None,
        minutes: Optional[int] = None,
        hours: Optional[int] = None,
        job_id: Optional[str] = None,
        **kwargs
    ) -> None:
        """Add a job that runs at regular intervals."""
        trigger = IntervalTrigger(
            seconds=seconds,
            minutes=minutes,
            hours=hours
        )
        
        self._scheduler.add_job(
            func,
            trigger=trigger,
            id=job_id,
            **kwargs
        )
        
        logger.info(f"Added interval job: {job_id or func.__name__}")

    def add_cron_job(
        self,
        func: Callable,
        cron_expression: str,
        job_id: Optional[str] = None,
        **kwargs
    ) -> None:
        """Add a job that runs on a cron schedule."""
        # Parse cron expression (simplified - assumes minute, hour, day, month, day_of_week)
        parts = cron_expression.split()
        if len(parts) != 5:
            raise ValueError("Cron expression must have 5 parts: minute hour day month day_of_week")
        
        minute, hour, day, month, day_of_week = parts
        
        trigger = CronTrigger(
            minute=minute if minute != "*" else None,
            hour=hour if hour != "*" else None,
            day=day if day != "*" else None,
            month=month if month != "*" else None,
            day_of_week=day_of_week if day_of_week != "*" else None,
        )
        
        self._scheduler.add_job(
            func,
            trigger=trigger,
            id=job_id,
            **kwargs
        )
        
        logger.info(f"Added cron job: {job_id or func.__name__} ({cron_expression})")

    def remove_job(self, job_id: str) -> None:
        """Remove a job by ID."""
        self._scheduler.remove_job(job_id)
        logger.info(f"Removed job: {job_id}")

    def list_jobs(self) -> Dict[str, Any]:
        """List all scheduled jobs."""
        jobs = {}
        for job in self._scheduler.get_jobs():
            jobs[job.id] = {
                "name": job.name,
                "next_run": job.next_run_time,
                "trigger": str(job.trigger),
            }
        return jobs
