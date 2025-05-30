"""
Orchestrator for managing the Fetch→Parse→Sink pipeline.
"""

import asyncio
import logging
from typing import Dict, List, Any, Optional
import yaml
from pathlib import Path

from .interfaces import Fetcher, Parser, Sink
from .models import ParsedItem
from .infra.scheduler import Scheduler


logger = logging.getLogger(__name__)


class DiffParser:
    """Stateful parser that only emits changed items."""
    
    def __init__(self):
        self._cache: Dict[str, Dict[str, Any]] = {}

    async def diff(self, item: ParsedItem) -> List[ParsedItem]:
        """Return the item only if it has changed."""
        cache_key = self._get_cache_key(item)
        current_content = item.content.copy()
        
        # Remove timestamp for comparison
        comparison_content = {k: v for k, v in current_content.items() if k != "timestamp"}
        
        if cache_key not in self._cache or self._cache[cache_key] != comparison_content:
            self._cache[cache_key] = comparison_content
            # Create a new item with .diff topic
            diff_item = item.copy(update={"topic": f"{item.topic}.diff"})
            return [diff_item]
        
        return []

    def _get_cache_key(self, item: ParsedItem) -> str:
        """Generate a cache key for the item."""
        if item.topic == "fi.short.aggregate":
            return f"agg:{item.content.get('lei', '')}"
        elif item.topic == "fi.short.positions":
            entity = item.content.get('entity_name', '')
            issuer = item.content.get('issuer_name', '')
            isin = item.content.get('isin', '')
            return f"pos:{entity}:{issuer}:{isin}"
        else:
            # Generic fallback
            return f"{item.topic}:{hash(str(sorted(item.content.items())))}"


class Pipeline:
    """A single Fetch→Parse→Sink pipeline."""
    
    def __init__(
        self,
        name: str,
        fetcher: Fetcher,
        parsers: List[Parser],
        sinks: List[Sink],
        use_diff: bool = True,
    ):
        self.name = name
        self.fetcher = fetcher
        self.parsers = parsers
        self.sinks = sinks
        self.use_diff = use_diff
        
        self._diff_parser = DiffParser() if use_diff else None
        self._running = False
        self._task: Optional[asyncio.Task] = None

    async def start(self) -> None:
        """Start the pipeline."""
        if self._running:
            return
        
        self._running = True
        self._task = asyncio.create_task(self._run())
        logger.info(f"Started pipeline: {self.name}")

    async def stop(self) -> None:
        """Stop the pipeline."""
        if not self._running:
            return
        
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        
        logger.info(f"Stopped pipeline: {self.name}")

    async def _run(self) -> None:
        """Run the pipeline."""
        try:
            async for raw_item in self.fetcher.fetch():
                if not self._running:
                    break
                
                # Parse the raw item
                for parser in self.parsers:
                    parsed_items = await parser.parse(raw_item)
                    
                    for parsed_item in parsed_items:
                        # Apply diff filtering if enabled
                        items_to_sink = [parsed_item]
                        if self._diff_parser:
                            diff_items = await self._diff_parser.diff(parsed_item)
                            items_to_sink = diff_items
                        
                        # Send to sinks
                        for item in items_to_sink:
                            for sink in self.sinks:
                                try:
                                    await sink.handle(item)
                                except Exception as e:
                                    logger.error(f"Sink {sink.name} failed to handle item: {e}")
        
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"Pipeline {self.name} failed: {e}")
            # Could implement restart logic here


class Orchestrator:
    """Main orchestrator for managing multiple pipelines."""
    
    def __init__(self, config_path: Optional[str] = None):
        self.config_path = Path(config_path) if config_path else Path("config.yaml")
        self.pipelines: List[Pipeline] = []
        self.scheduler = Scheduler()
        self._running = False

    async def load_config(self) -> Dict[str, Any]:
        """Load configuration from YAML file."""
        if not self.config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {self.config_path}")
        
        with open(self.config_path, 'r') as f:
            return yaml.safe_load(f)

    async def register_pipeline(self, pipeline: Pipeline) -> None:
        """Register a pipeline with the orchestrator."""
        self.pipelines.append(pipeline)
        logger.info(f"Registered pipeline: {pipeline.name}")

    async def start(self) -> None:
        """Start the orchestrator and all pipelines."""
        if self._running:
            return
        
        self._running = True
        
        # Start scheduler
        await self.scheduler.start()
        
        # Start all pipelines
        for pipeline in self.pipelines:
            await pipeline.start()
        
        logger.info(f"Started orchestrator with {len(self.pipelines)} pipelines")

    async def stop(self) -> None:
        """Stop the orchestrator and all pipelines."""
        if not self._running:
            return
        
        self._running = False
        
        # Stop all pipelines
        for pipeline in self.pipelines:
            await pipeline.stop()
        
        # Stop scheduler
        await self.scheduler.stop()
        
        logger.info("Stopped orchestrator")

    async def run_forever(self) -> None:
        """Run the orchestrator until interrupted."""
        await self.start()
        try:
            while self._running:
                await asyncio.sleep(1)
        except KeyboardInterrupt:
            logger.info("Received interrupt signal")
        finally:
            await self.stop()
