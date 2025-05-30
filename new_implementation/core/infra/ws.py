"""
WebSocket client infrastructure with heartbeat and reconnection.
"""

import asyncio
import json
import logging
from typing import Any, Callable, Dict, Optional

import aiohttp


logger = logging.getLogger(__name__)


class WebSocketClient:
    """Async WebSocket client with heartbeat and reconnection."""
    
    def __init__(
        self,
        url: str,
        heartbeat_interval: float = 30.0,
        reconnect_delay: float = 5.0,
        max_reconnect_attempts: int = 10,
    ):
        self.url = url
        self.heartbeat_interval = heartbeat_interval
        self.reconnect_delay = reconnect_delay
        self.max_reconnect_attempts = max_reconnect_attempts
        
        self._session: Optional[aiohttp.ClientSession] = None
        self._ws: Optional[aiohttp.ClientWebSocketResponse] = None
        self._connected = False
        self._reconnect_count = 0
        self._heartbeat_task: Optional[asyncio.Task] = None
        self._listen_task: Optional[asyncio.Task] = None
        
        # Event handlers
        self.on_message: Optional[Callable[[str], None]] = None
        self.on_connect: Optional[Callable[[], None]] = None
        self.on_disconnect: Optional[Callable[[], None]] = None
        self.on_error: Optional[Callable[[Exception], None]] = None

    async def connect(self) -> None:
        """Connect to the WebSocket."""
        if self._connected:
            return
        
        try:
            if not self._session:
                self._session = aiohttp.ClientSession()
            
            self._ws = await self._session.ws_connect(self.url)
            self._connected = True
            self._reconnect_count = 0
            
            logger.info(f"Connected to WebSocket: {self.url}")
            
            if self.on_connect:
                self.on_connect()
            
            # Start heartbeat and message listening
            self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
            self._listen_task = asyncio.create_task(self._listen_loop())
            
        except Exception as e:
            logger.error(f"Failed to connect to WebSocket: {e}")
            if self.on_error:
                self.on_error(e)
            await self._schedule_reconnect()

    async def disconnect(self) -> None:
        """Disconnect from the WebSocket."""
        self._connected = False
        
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
        if self._listen_task:
            self._listen_task.cancel()
        
        if self._ws and not self._ws.closed:
            await self._ws.close()
        
        if self._session:
            await self._session.close()
            self._session = None
        
        logger.info("Disconnected from WebSocket")
        
        if self.on_disconnect:
            self.on_disconnect()

    async def send_text(self, message: str) -> None:
        """Send a text message."""
        if self._ws and not self._ws.closed:
            await self._ws.send_str(message)
        else:
            logger.warning("Cannot send message: WebSocket not connected")

    async def send_json(self, data: Dict[str, Any]) -> None:
        """Send a JSON message."""
        await self.send_text(json.dumps(data))

    async def _heartbeat_loop(self) -> None:
        """Send periodic heartbeat messages."""
        while self._connected:
            try:
                if self._ws and not self._ws.closed:
                    await self._ws.ping()
                await asyncio.sleep(self.heartbeat_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Heartbeat error: {e}")
                break

    async def _listen_loop(self) -> None:
        """Listen for incoming messages."""
        while self._connected:
            try:
                if not self._ws:
                    break
                
                msg = await self._ws.receive()
                
                if msg.type == aiohttp.WSMsgType.TEXT:
                    if self.on_message:
                        self.on_message(msg.data)
                elif msg.type == aiohttp.WSMsgType.ERROR:
                    logger.error(f"WebSocket error: {self._ws.exception()}")
                    break
                elif msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSING):
                    logger.info("WebSocket connection closed")
                    break
                    
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Listen loop error: {e}")
                if self.on_error:
                    self.on_error(e)
                break
        
        self._connected = False
        await self._schedule_reconnect()

    async def _schedule_reconnect(self) -> None:
        """Schedule a reconnection attempt."""
        if self._reconnect_count >= self.max_reconnect_attempts:
            logger.error(f"Max reconnect attempts ({self.max_reconnect_attempts}) reached")
            return
        
        self._reconnect_count += 1
        delay = self.reconnect_delay * (2 ** (self._reconnect_count - 1))  # Exponential backoff
        
        logger.info(f"Scheduling reconnect attempt {self._reconnect_count} in {delay}s")
        await asyncio.sleep(delay)
        await self.connect()
