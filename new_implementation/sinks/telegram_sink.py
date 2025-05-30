"""
Telegram sink for sending notifications to Telegram channels.
"""

import logging
from typing import Optional

try:
    import telegram
    from telegram import Bot
except ImportError:
    telegram = None
    Bot = None

from ..core.interfaces import Sink
from ..core.models import ParsedItem


logger = logging.getLogger(__name__)


class TelegramSink(Sink):
    """Sink that sends notifications to Telegram channels."""
    
    name = "TelegramSink"
    
    def __init__(
        self,
        bot_token: Optional[str] = None,
        chat_id: Optional[str] = None,
    ):
        if telegram is None:
            raise ImportError("python-telegram-bot package is required for Telegram notifications")
        
        self.bot = Bot(token=bot_token) if bot_token else None
        self.chat_id = chat_id

    async def handle(self, item: ParsedItem) -> None:
        """Handle a parsed item by sending Telegram notification."""
        if not self.bot or not self.chat_id:
            logger.warning("Telegram bot or chat_id not configured, skipping notification")
            return
        
        try:
            if item.topic == "fi.short.aggregate.diff":
                await self._handle_aggregate_diff(item)
            elif item.topic == "fi.short.positions.diff":
                await self._handle_position_diff(item)
            else:
                logger.debug(f"No Telegram handler for topic: {item.topic}")
        
        except Exception as e:
            logger.error(f"Failed to send Telegram notification: {e}")

    async def _handle_aggregate_diff(self, item: ParsedItem) -> None:
        """Handle aggregate short interest changes."""
        company_name = item.content.get("company_name", "Unknown")
        position_percent = item.content.get("position_percent", 0)
        
        message = f"🔴 *Short Interest Update*\n\n"
        message += f"**Company:** {company_name}\n"
        message += f"**Position:** {position_percent:.2f}%\n"
        message += f"**Updated:** {item.content.get('latest_position_date', 'Unknown')}"
        
        await self.bot.send_message(
            chat_id=self.chat_id,
            text=message,
            parse_mode="Markdown"
        )

    async def _handle_position_diff(self, item: ParsedItem) -> None:
        """Handle individual position changes."""
        issuer_name = item.content.get("issuer_name", "Unknown")
        entity_name = item.content.get("entity_name", "Unknown")
        position_percent = item.content.get("position_percent", 0)
        
        # Only send if position is significant
        if position_percent >= 0.5:  # 0.5% threshold
            message = f"📊 *Position Update*\n\n"
            message += f"**Issuer:** {issuer_name}\n"
            message += f"**Entity:** {entity_name}\n"
            message += f"**Position:** {position_percent:.2f}%\n"
            message += f"**Date:** {item.content.get('position_date', 'Unknown')}"
            
            await self.bot.send_message(
                chat_id=self.chat_id,
                text=message,
                parse_mode="Markdown"
            )
