"""
Integration bridge for connecting the new modular implementation with existing Discord bot.
"""

import asyncio
import logging
from typing import Optional, Set

import aiohttp

from core.orchestrator import Orchestrator, Pipeline
from plugins.fi_shortinterest.fetcher import FiFetcher
from plugins.fi_shortinterest.parser import FiAggParser, FiActParser
from sinks.database_sink import DatabaseSink
from sinks.discord_sink import DiscordSink


logger = logging.getLogger(__name__)


class IntegrationBridge:
    """Bridge between the new modular system and existing Discord bot."""
    
    def __init__(
        self,
        discord_bot=None,
        channel_id: Optional[int] = None,
        error_channel_id: Optional[int] = None,
        tracked_companies: Optional[Set[str]] = None,
        db_path: str = "scraper.db",
    ):
        self.discord_bot = discord_bot
        self.channel_id = channel_id
        self.error_channel_id = error_channel_id
        self.tracked_companies = tracked_companies or set()
        self.db_path = db_path
        
        self.orchestrator: Optional[Orchestrator] = None
        self._running = False

    async def start(
        self,
        session: Optional[aiohttp.ClientSession] = None,
    ) -> None:
        """Start the FI short interest monitoring."""
        if self._running:
            logger.warning("Integration bridge already running")
            return
        
        logger.info("Starting FI short interest integration bridge...")
        
        # Create orchestrator
        self.orchestrator = Orchestrator()
        
        # Create pipeline components
        fetcher = FiFetcher(session=session)
        parsers = [FiAggParser(), FiActParser()]
        
        # Create sinks
        sinks = [DatabaseSink(self.db_path)]
        
        # Add Discord sink if bot is available
        if self.discord_bot:
            discord_sink = DiscordSink(
                bot=self.discord_bot,
                channel_id=self.channel_id,
                error_channel_id=self.error_channel_id,
                tracked_companies=self.tracked_companies,
            )
            sinks.append(discord_sink)
        
        # Create and register pipeline
        pipeline = Pipeline(
            name="fi_shortinterest_bridge",
            fetcher=fetcher,
            parsers=parsers,
            sinks=sinks,
            use_diff=True,
        )
        
        await self.orchestrator.register_pipeline(pipeline)
        await self.orchestrator.start()
        
        self._running = True
        logger.info("FI short interest monitoring started")

    async def stop(self) -> None:
        """Stop the FI short interest monitoring."""
        if not self._running:
            return
        
        logger.info("Stopping FI short interest integration bridge...")
        
        if self.orchestrator:
            await self.orchestrator.stop()
        
        self._running = False
        logger.info("FI short interest monitoring stopped")

    def is_running(self) -> bool:
        """Check if the bridge is running."""
        return self._running

    async def manual_update(self) -> None:
        """Trigger a manual update (for compatibility with existing code)."""
        if not self._running:
            logger.warning("Cannot trigger manual update - bridge not running")
            return
        
        logger.info("Manual update requested - the new system runs continuously")
        # The new system runs continuously, so no manual trigger is needed
        # This method is kept for backward compatibility


# Convenience function for easy integration with existing bot code
async def start_fi_monitoring(
    discord_bot=None,
    session: Optional[aiohttp.ClientSession] = None,
    channel_id: int = 1175019650963222599,
    error_channel_id: int = 1162053416290361516,
    tracked_companies: Optional[Set[str]] = None,
    db_path: str = "scraper.db",
) -> IntegrationBridge:
    """
    Start FI short interest monitoring with the new modular system.
    
    This is a drop-in replacement for the old fi_blankning module.
    """
    if tracked_companies is None:
        tracked_companies = {
            'Embracer Group AB', 'Paradox Interactive AB (publ)', 'Starbreeze AB',
            'EG7', 'Enad Global 7', 'Maximum Entertainment', 'MAG Interactive',
            'G5 Entertainment AB (publ)', 'Modern Times Group MTG AB', 'Thunderful',
            'MGI - Media and Games Invest SE', 'Stillfront Group AB (publ)'
        }
    
    bridge = IntegrationBridge(
        discord_bot=discord_bot,
        channel_id=channel_id,
        error_channel_id=error_channel_id,
        tracked_companies=tracked_companies,
        db_path=db_path,
    )
    
    await bridge.start(session=session)
    return bridge