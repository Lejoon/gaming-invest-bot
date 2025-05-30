#!/usr/bin/env python3
"""
Test script for the new FI short interest implementation.
Tests the complete Fetcher→Parser→Sink pipeline.
"""

import asyncio
import logging
import sys
from pathlib import Path
from typing import List

# Add the current directory to Python path
sys.path.insert(0, str(Path(__file__).parent))

from core.orchestrator import Orchestrator
from core.infra.db import Database
from core.models import ParsedItem, Event
from plugins.fi_shortinterest import FiFetcher, FiAggParser, FiActParser
from sinks.database_sink import DatabaseSink

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def test_database_setup():
    """Test database initialization and table creation."""
    logger.info("Testing database setup...")
    
    db_manager = Database("test_fi_shortinterest.db")
    await db_manager.initialize()
    
    # Check if tables were created
    async with db_manager.get_connection() as conn:
        cursor = await conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )
        tables = await cursor.fetchall()
        table_names = [table[0] for table in tables]
        
        logger.info(f"Created tables: {table_names}")
        
        expected_tables = ['raw_items', 'parsed_items', 'events', 'fi_aggregate', 'fi_positions']
        for table in expected_tables:
            if table in table_names:
                logger.info(f"✓ Table '{table}' exists")
            else:
                logger.warning(f"✗ Table '{table}' missing")
    
    await db_manager.close()
    logger.info("Database setup test completed")


async def test_fetcher():
    """Test the FI fetcher component."""
    logger.info("Testing FI fetcher...")
    
    fetcher = FiFetcher()
    
    try:
        raw_items = await fetcher.fetch()
        logger.info(f"Fetched {len(raw_items)} raw items")
        
        for item in raw_items:
            logger.info(f"Raw item: {item.source} - {len(item.data)} bytes")
            
        return raw_items
        
    except Exception as e:
        logger.error(f"Fetcher test failed: {e}")
        return []


async def test_parsers(raw_items: List):
    """Test the FI parser components."""
    logger.info("Testing FI parsers...")
    
    agg_parser = FiAggParser()
    act_parser = FiActParser()
    
    all_parsed = []
    
    for raw_item in raw_items:
        if "blankning_aggregerad" in raw_item.source:
            logger.info(f"Parsing aggregate data from {raw_item.source}")
            parsed_items = await agg_parser.parse(raw_item)
            logger.info(f"Parsed {len(parsed_items)} aggregate items")
            all_parsed.extend(parsed_items)
            
        elif "blankning_aktspecifik" in raw_item.source:
            logger.info(f"Parsing position data from {raw_item.source}")
            parsed_items = await act_parser.parse(raw_item)
            logger.info(f"Parsed {len(parsed_items)} position items")
            all_parsed.extend(parsed_items)
    
    # Show sample parsed data
    if all_parsed:
        logger.info("Sample parsed data:")
        for i, item in enumerate(all_parsed[:3]):  # Show first 3 items
            logger.info(f"  Item {i+1}: {item.item_type} - {item.data}")
    
    return all_parsed


async def test_database_sink(parsed_items: List[ParsedItem]):
    """Test the database sink."""
    logger.info("Testing database sink...")
    
    db_manager = Database("test_fi_shortinterest.db")
    await db_manager.initialize()
    
    db_sink = DatabaseSink(db_manager)
    
    # Test processing parsed items
    events = []
    for item in parsed_items:
        item_events = await db_sink.process(item)
        events.extend(item_events)
    
    logger.info(f"Database sink generated {len(events)} events")
    
    # Check what was inserted
    async with db_manager.get_connection() as conn:
        cursor = await conn.execute("SELECT COUNT(*) FROM fi_aggregate")
        agg_count = (await cursor.fetchone())[0]
        
        cursor = await conn.execute("SELECT COUNT(*) FROM fi_positions")
        pos_count = (await cursor.fetchone())[0]
        
        logger.info(f"Database contains {agg_count} aggregate records, {pos_count} position records")
    
    await db_manager.close()
    return events


async def test_orchestrator():
    """Test the complete orchestrator pipeline."""
    logger.info("Testing orchestrator pipeline...")
    
    # Create orchestrator with test database
    db_manager = Database("test_fi_shortinterest.db")
    orchestrator = Orchestrator(db_manager)
    
    # Register components
    fetcher = FiFetcher()
    agg_parser = FiAggParser()
    act_parser = FiActParser()
    db_sink = DatabaseSink(db_manager)
    
    orchestrator.register_fetcher("fi_shortinterest", fetcher)
    orchestrator.register_parser("fi_agg", agg_parser)
    orchestrator.register_parser("fi_act", act_parser)
    orchestrator.register_sink("database", db_sink)
    
    # Configure pipeline
    pipeline_config = {
        "fetcher": "fi_shortinterest",
        "parsers": {
            "blankning_aggregerad": "fi_agg",
            "blankning_aktspecifik": "fi_act"
        },
        "sinks": ["database"]
    }
    
    # Run pipeline
    try:
        events = await orchestrator.run_pipeline("fi_shortinterest", pipeline_config)
        logger.info(f"Pipeline completed with {len(events)} events")
        
        # Show events
        for event in events[:5]:  # Show first 5 events
            logger.info(f"Event: {event.event_type} - {event.message}")
            
        return events
        
    except Exception as e:
        logger.error(f"Pipeline test failed: {e}")
        return []


async def test_diff_detection():
    """Test diff detection by running the pipeline twice."""
    logger.info("Testing diff detection...")
    
    db_manager = Database("test_fi_shortinterest.db")
    orchestrator = Orchestrator(db_manager)
    
    # Register components
    fetcher = FiFetcher()
    agg_parser = FiAggParser()
    act_parser = FiActParser()
    db_sink = DatabaseSink(db_manager)
    
    orchestrator.register_fetcher("fi_shortinterest", fetcher)
    orchestrator.register_parser("fi_agg", agg_parser)
    orchestrator.register_parser("fi_act", act_parser)
    orchestrator.register_sink("database", db_sink)
    
    pipeline_config = {
        "fetcher": "fi_shortinterest",
        "parsers": {
            "blankning_aggregerad": "fi_agg",
            "blankning_aktspecifik": "fi_act"
        },
        "sinks": ["database"]
    }
    
    # First run
    logger.info("First pipeline run...")
    events1 = await orchestrator.run_pipeline("fi_shortinterest", pipeline_config)
    logger.info(f"First run: {len(events1)} events")
    
    # Second run (should detect no changes)
    logger.info("Second pipeline run...")
    events2 = await orchestrator.run_pipeline("fi_shortinterest", pipeline_config)
    logger.info(f"Second run: {len(events2)} events")
    
    if len(events2) == 0:
        logger.info("✓ Diff detection working - no duplicate processing")
    else:
        logger.warning("✗ Diff detection may not be working properly")


async def cleanup_test_files():
    """Clean up test database files."""
    test_files = [
        "test_fi_shortinterest.db",
        "test_fi_shortinterest.db-wal",
        "test_fi_shortinterest.db-shm"
    ]
    
    for file_name in test_files:
        file_path = Path(file_name)
        if file_path.exists():
            file_path.unlink()
            logger.info(f"Cleaned up {file_name}")


async def main():
    """Run all tests."""
    logger.info("Starting FI short interest implementation tests...")
    
    try:
        # Test 1: Database setup
        await test_database_setup()
        
        # Test 2: Fetcher
        raw_items = await test_fetcher()
        if not raw_items:
            logger.error("Fetcher test failed, skipping remaining tests")
            return
        
        # Test 3: Parsers
        parsed_items = await test_parsers(raw_items)
        if not parsed_items:
            logger.error("Parser test failed, skipping remaining tests")
            return
        
        # Test 4: Database sink
        events = await test_database_sink(parsed_items)
        
        # Test 5: Complete orchestrator pipeline
        await test_orchestrator()
        
        # Test 6: Diff detection
        await test_diff_detection()
        
        logger.info("All tests completed successfully!")
        
    except Exception as e:
        logger.error(f"Test suite failed: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        # Cleanup
        await cleanup_test_files()


if __name__ == "__main__":
    asyncio.run(main())