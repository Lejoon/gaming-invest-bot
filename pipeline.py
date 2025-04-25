import abc
import asyncio
import discord
from datetime import datetime, timedelta
from typing import Any, List


class BasePipeline(abc.ABC):
    """
    Abstract base class for data pipelines. Supports both DB and Discord pipelines.
    """
    def __init__(
        self,
        name: str,
        db=None,
        table: str = None,
        bot=None,
        channel_id: int = None,
        interval_hours: int = None,
        run_at_hour: int = None,
    ):
        self.name = name
        self.db = db
        self.table = table
        self.bot = bot
        self.channel_id = channel_id
        self.interval_hours = interval_hours
        self.run_at_hour = run_at_hour

    @abc.abstractmethod
    async def fetch(self) -> Any:
        """
        Fetch raw data (HTML, JSON, WebSocket msgs, etc.)
        """
        pass

    def parse(self, raw: Any) -> Any:
        """
        Convert raw data into structured items or messages.
        Default: no-op.
        """
        return raw

    async def store(self, items: Any) -> Any:
        """
        Default storage logic:
        - If db + table provided: bulk insert with hourly dedupe.
        - If bot + channel_id provided: send Discord messages/embeds.
        """
        # DB pipeline
        if self.db and self.table:
            latest_ts = self.db.get_latest_timestamp(self.table)
            if latest_ts:
                last_dt = datetime.strptime(latest_ts, '%Y-%m-%d %H')
                now = datetime.now().replace(minute=0, second=0, microsecond=0)
                if now - last_dt < timedelta(hours=1):
                    return items
            # insert_bulk_data accepts list of dicts and optional table name
            self.db.insert_bulk_data(items, table=self.table)
            return items

        # Discord pipeline
        if self.bot and self.channel_id:
            channel = self.bot.get_channel(self.channel_id)
            if channel:
                for item in items:
                    if isinstance(item, discord.Embed):
                        await channel.send(embed=item)
                    else:
                        await channel.send(item)
            return items

        return items

    async def run(self) -> Any:
        raw = await self.fetch()
        parsed = self.parse(raw)
        return await self.store(parsed)


async def schedule_pipeline(pipeline: BasePipeline):
    """
    Schedule a pipeline:
    - If run_at_hour is set: runs at that hour daily.
    - Else if interval_hours is set: runs on the given interval.
    """
    while True:
        now = datetime.now()
        if pipeline.run_at_hour is not None:
            # compute secs until next run_at_hour
            next_run = now.replace(hour=pipeline.run_at_hour, minute=0, second=0, microsecond=0)
            if next_run <= now:
                next_run += timedelta(days=1)
            sleep_secs = (next_run - now).total_seconds()
        elif pipeline.interval_hours is not None:
            sleep_secs = pipeline.interval_hours * 3600
        else:
            sleep_secs = 3600

        await asyncio.sleep(sleep_secs)
        try:
            await pipeline.run()
        except Exception as e:
            # default error handling
            print(f"Error running pipeline {pipeline.name}: {e}")
