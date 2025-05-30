"""
Discord sink for sending notifications to Discord channels.
"""

import logging
from typing import Optional, Set

try:
    import discord
    from discord import Embed
except ImportError:
    discord = None
    Embed = None

from ..core.interfaces import Sink
from ..core.models import ParsedItem


logger = logging.getLogger(__name__)


class DiscordSink(Sink):
    """Sink that sends notifications to Discord channels."""
    
    name = "DiscordSink"
    
    def __init__(
        self,
        bot: Optional[object] = None,
        channel_id: Optional[int] = None,
        error_channel_id: Optional[int] = None,
        tracked_companies: Optional[Set[str]] = None,
    ):
        if discord is None:
            raise ImportError("discord.py package is required for Discord notifications")
        
        self.bot = bot
        self.channel_id = channel_id
        self.error_channel_id = error_channel_id
        self.tracked_companies = tracked_companies or set()

    async def handle(self, item: ParsedItem) -> None:
        """Handle a parsed item by sending Discord notification."""
        if not self.bot or not self.channel_id:
            logger.warning("Discord bot or channel not configured, skipping notification")
            return
        
        try:
            if item.topic == "fi.short.aggregate.diff":
                await self._handle_aggregate_diff(item)
            elif item.topic == "fi.short.positions.diff":
                await self._handle_position_diff(item)
            else:
                logger.debug(f"No Discord handler for topic: {item.topic}")
        
        except Exception as e:
            logger.error(f"Failed to send Discord notification: {e}")
            await self._send_error_notification(e)

    async def _handle_aggregate_diff(self, item: ParsedItem) -> None:
        """Handle aggregate short interest changes."""
        company_name = item.content.get("company_name", "Unknown")
        
        # Only notify for tracked companies
        if self.tracked_companies and company_name not in self.tracked_companies:
            return
        
        position_percent = item.content.get("position_percent", 0)
        lei = item.content.get("lei", "")
        
        embed = Embed(
            title="🔴 Short Interest Update",
            description=f"**{company_name}**",
            color=0xff0000 if position_percent > 5.0 else 0xff8c00
        )
        
        embed.add_field(
            name="Position",
            value=f"{position_percent:.2f}%",
            inline=True
        )
        
        embed.add_field(
            name="LEI",
            value=lei[:20] + "..." if len(lei) > 20 else lei,
            inline=True
        )
        
        embed.add_field(
            name="Updated",
            value=item.content.get("latest_position_date", "Unknown"),
            inline=True
        )
        
        channel = self.bot.get_channel(self.channel_id)
        if channel:
            await channel.send(embed=embed)
        else:
            logger.error(f"Could not find Discord channel: {self.channel_id}")

    async def _handle_position_diff(self, item: ParsedItem) -> None:
        """Handle individual position changes."""
        issuer_name = item.content.get("issuer_name", "Unknown")
        
        # Only notify for tracked companies
        if self.tracked_companies and issuer_name not in self.tracked_companies:
            return
        
        entity_name = item.content.get("entity_name", "Unknown")
        position_percent = item.content.get("position_percent", 0)
        
        embed = Embed(
            title="📊 Position Update",
            description=f"**{issuer_name}**",
            color=0x0099ff
        )
        
        embed.add_field(
            name="Entity",
            value=entity_name[:30] + "..." if len(entity_name) > 30 else entity_name,
            inline=False
        )
        
        embed.add_field(
            name="Position",
            value=f"{position_percent:.2f}%",
            inline=True
        )
        
        embed.add_field(
            name="Date",
            value=item.content.get("position_date", "Unknown"),
            inline=True
        )
        
        # Only send if position is significant
        if position_percent >= 0.5:  # 0.5% threshold
            channel = self.bot.get_channel(self.channel_id)
            if channel:
                await channel.send(embed=embed)

    async def _send_error_notification(self, exception: Exception) -> None:
        """Send error notification to error channel."""
        if not self.error_channel_id:
            return
        
        error_channel = self.bot.get_channel(self.error_channel_id)
        if error_channel:
            error_message = f"Discord sink error: {type(exception).__name__}: {exception}"
            await error_channel.send(error_message)
