"""
Main entry point for the scraper platform.
"""

import asyncio
import logging
import sys
from pathlib import Path

from core.orchestrator import Orchestrator, Pipeline
from core.infra.db import Database
from plugins.fi_shortinterest.fetcher import FiFetcher
from plugins.fi_shortinterest.parser import FiAggParser, FiActParser
from sinks.database_sink import DatabaseSink


async def main():
    """Main entry point."""
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(name)s | %(levelname)s | %(message)s"
    )
    logger = logging.getLogger(__name__)
    
    # Create orchestrator
    orchestrator = Orchestrator("config.yaml")
    
    # Create and register FI short interest pipeline
    fetcher = FiFetcher()
    parsers = [FiAggParser(), FiActParser()]
    sinks = [DatabaseSink("scraper.db")]
    
    pipeline = Pipeline(
        name="fi_shortinterest",
        fetcher=fetcher,
        parsers=parsers,
        sinks=sinks,
        use_diff=True
    )
    
    await orchestrator.register_pipeline(pipeline)
    
    # Run the orchestrator
    logger.info("Starting scraper platform...")
    try:
        await orchestrator.run_forever()
    except KeyboardInterrupt:
        logger.info("Shutting down...")
    finally:
        await orchestrator.stop()
        
        # Close database connections
        for sink in sinks:
            if hasattr(sink, 'close'):
                await sink.close()


if __name__ == "__main__":
    asyncio.run(main())