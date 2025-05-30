# Modular Scraper Platform

This is a complete rewrite of the financial data scraping system using a modular Fetcher→Parser→Sink architecture.

## Architecture

```
new_implementation/
│
├── core/                     # Re-usable framework – very stable
│   ├── models.py             # RawItem, ParsedItem, Event
│   ├── interfaces.py         # Fetcher, Parser, Sink abstract classes
│   ├── infra/
│   │   ├── http.py           # aiohttp wrapper with retry logic
│   │   ├── ws.py             # websocket client with heartbeat / reconnect
│   │   ├── sel.py            # Playwright helpers for browser automation
│   │   ├── db.py             # async SQLite wrapper with migrations
│   │   └── scheduler.py      # APScheduler wrapper
│   └── orchestrator.py       # pipeline management and orchestration
│
├── plugins/                  # Domain-specific implementations
│   └── fi_shortinterest/
│       ├── fetcher.py        # FiFetcher (ODS download + timestamp poll)
│       ├── parser.py         # FiAggParser / FiActParser
│       └── sinkmap.yaml      # default sink configuration
│
├── sinks/                    # Cross-cutting outputs
│   ├── discord_sink.py       # Discord notifications
│   ├── database_sink.py      # SQLite persistence
│   └── telegram_sink.py      # Telegram notifications
│
├── config.yaml               # declarative service configuration
├── main.py                   # standalone entry point
└── integration_bridge.py     # bridge for existing Discord bot
```

## Key Features

- **Modular Design**: Clean separation between fetching, parsing, and sinking
- **Async/Await**: Fully asynchronous with proper error handling
- **Database**: SQLite with automatic migrations and connection pooling
- **Retry Logic**: Robust HTTP client with exponential backoff
- **Diff Detection**: Only processes/notifies on actual changes
- **Type Safety**: Pydantic models throughout for validation
- **Extensible**: Easy to add new data sources and output channels

## Quick Start

### Standalone Usage

```bash
cd new_implementation
pip install -r requirements.txt
python main.py
```

### Integration with Existing Discord Bot

```python
from new_implementation.integration_bridge import start_fi_monitoring

# In your Discord bot setup
bridge = await start_fi_monitoring(
    discord_bot=bot,
    session=your_aiohttp_session,  # optional
    channel_id=1175019650963222599,
    error_channel_id=1162053416290361516,
)

# The system now runs continuously in the background
# No need for manual update calls
```

## Configuration

Edit `config.yaml` to customize:
- Database path
- Polling intervals
- Discord channels
- Tracked companies
- Logging levels

## Database Schema

The system automatically creates and migrates SQLite tables:

- `short_positions`: Aggregate short interest data
- `position_holders`: Individual position holder data
- `migrations`: Schema version tracking

## Adding New Data Sources

1. Create a new plugin directory under `plugins/`
2. Implement `Fetcher` and `Parser` classes
3. Add configuration to `config.yaml`
4. Register with the orchestrator

## Development

```bash
# Install development dependencies
pip install -r requirements.txt

# Run tests
pytest

# Format code
black .
isort .

# Type checking
mypy .
```

## Migration from Old System

The `integration_bridge.py` provides a drop-in replacement for the old `fi_blankning.py` module. Simply replace your existing import and initialization code.