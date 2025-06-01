# Gaming Invest Bot Framework Analysis

## Current Architecture Assessment

Your gaming-invest-bot project demonstrates a mixed architecture with both monolithic and domain-specific patterns. Here's how your existing modules map to the proposed framework:

## 1. Framework Mapping Analysis

### Current "Orchestrator" (main.py)
**Role:** Discord bot + manual task scheduler
**Framework fit:** ❌ **Anti-pattern**
- Single entrypoint mixing Discord commands with background task management
- Hard-coded task initialization in `on_ready()`
- No separation between command handling and data pipeline orchestration
- Tasks managed as global variables with manual lifecycle management

**Recommended transformation:**
```python
# Instead of main.py doing everything, split into:
# 1. discord_bot.py - Pure Discord command interface
# 2. scheduler.py - Background task orchestration
# 3. config.yaml - Service definitions
```

### Current Data Sources Analysis

| Module | Framework Role | Current Pattern | Conformance Score | Notes |
|--------|---------------|-----------------|-------------------|-------|
| **fi_blankning.py** | Fetcher + Parser + Sink | ✅ Close fit | 8/10 | Already has distinct fetch/parse phases, just needs interface cleanup |
| **mfn.py** | Fetcher (WebSocket) | ⚠️ Mixed concerns | 6/10 | WebSocket handling + Discord notifications in one place |
| **placera.py** | Fetcher + Parser | ⚠️ Mixed concerns | 5/10 | HTTP scraping + content filtering + Discord posting |
| **steam.py** | Mixed Pipeline | ❌ Anti-pattern | 4/10 | Contains commands, scraping, database, and pipeline logic |
| **psstore.py** | Fetcher + Parser | ⚠️ Mixed concerns | 6/10 | HTTP scraping with some separation |
| **ig.py** | Fetcher (Selenium) | ⚠️ Mixed concerns | 5/10 | Selenium automation + Discord notifications |

### Infrastructure Modules

| Module | Framework Role | Assessment | Notes |
|--------|---------------|------------|-------|
| **database.py** | ✅ Good Sink pattern | 7/10 | Already provides abstraction but could be more generic |
| **chart.py** | ❌ Command + Data + UI | 3/10 | Mixes Avanza API, charting, and Discord presentation |
| **avanzaauth.py** | ✅ Good Infra pattern | 8/10 | Clean authentication service with session management |

## 2. Specific Module Analysis

### ✅ Well-structured: fi_blankning.py
**Current strengths:**
- Clear separation: `fetch_url()` → `read_aggregate_data()` → `send_embed()`
- Retry decorators on HTTP operations
- Configuration constants at top
- Database abstraction usage

**Framework alignment:**
```python
# Already close to ideal:
class FiBlankningSink(Sink):
    async def handle(self, item: ParsedItem) -> None:
        # send_embed() logic here
        
class FiBlankningFetcher(Fetcher):
    async def fetch(self) -> AsyncIterator[RawItem]:
        # download_file() + timestamp checking logic
        
class FiBlankningParser(Parser):
    async def parse(self, raw: RawItem) -> List[ParsedItem]:
        # read_aggregate_data() logic
```

### ⚠️ Mixed concerns: mfn.py
**Current pattern:**
```python
async def fetch_mfn_updates(bot):  # ❌ Fetcher+Sink coupled
    async with websockets.connect() as ws:
        message = await ws.recv()  # ✅ Good fetching
        soup = BeautifulSoup(message)  # ✅ Good parsing  
        await channel.send(embed)  # ❌ Direct Discord coupling
```

**Framework transformation:**
```python
class MfnWebSocketFetcher(WebSocketFetcher):
    URI = 'wss://mfn.se/all/?filter=...'
    
class MfnPressReleaseParser(Parser):
    async def parse(self, raw: RawItem) -> List[ParsedItem]:
        soup = BeautifulSoup(raw.payload)
        # Extract press release data
        
class DiscordSink(Sink):
    async def handle(self, item: ParsedItem) -> None:
        # Generic Discord embedding logic
```

### ❌ Anti-pattern: steam.py
**Issues:**
- Contains Discord commands (`gts_command`)
- Database operations mixed with API calls
- Pipeline class mixed with command handlers
- No clear separation of concerns

**Needs complete refactor:**
```python
# Split into:
# steam_fetcher.py - SteamApiFetcher, SteamScrapeFetcher
# steam_parser.py - TopGamesParser, CCUParser  
# steam_commands.py - Discord command handlers
# steam_pipeline.py - SteamPipeline(BasePipeline)
```

## 3. Proposed Refactoring Roadmap

### Phase 1: Extract Interfaces (Low Risk)
1. Create `core/interfaces.py` with `Fetcher`, `Parser`, `Sink` ABC classes
2. Create `infra/` modules for shared services:
   - `http.py` - aiohttp wrapper with retry logic
   - `ws.py` - WebSocket manager  
   - `sel.py` - Selenium helpers
   - `discord_sink.py` - Generic Discord notification sink

### Phase 2: Refactor Best Candidates (Medium Risk)
1. **fi_blankning.py** → Extract into clean Fetcher/Parser/Sink trio
2. **database.py** → Convert to generic `DatabaseSink` implementation
3. **avanzaauth.py** → Move to `infra/auth.py`

### Phase 3: Tackle Complex Modules (High Risk)
1. **steam.py** → Split into multiple focused modules
2. **chart.py** → Separate Avanza integration from chart generation
3. **main.py** → Split into `discord_bot.py` + `orchestrator.py`

### Phase 4: Configuration-Driven (High Value)
```yaml
# config.yaml
services:
  fi_short_sellers:
    fetcher: fi_shortinterest.FiBlankningSinkFetcher
    parser: fi_shortinterest.FiBlankningParser  
    sinks: [DatabaseSink, DiscordSink]
    schedule: "0 */30 * * * *"
    
  mfn_press_releases:
    fetcher: mfn.MfnWebSocketFetcher
    parser: mfn.MfnPressReleaseParser
    sinks: [DiscordSink]
    # WebSocket = no schedule needed
```

## 4. Immediate Quick Wins

### 1. Extract Common HTTP Logic
```python
# infra/http.py
@aiohttp_retry(retries=5, base_delay=5.0, max_delay=120.0)  # Move decorator here
async def fetch_url(session, url, **kwargs):
    async with session.get(url, **kwargs) as response:
        return await response.read()
```

### 2. Generic Discord Sink
```python
# sinks/discord_sink.py  
class DiscordSink(Sink):
    def __init__(self, bot, channel_id):
        self.bot = bot
        self.channel_id = channel_id
        
    async def handle(self, item: ParsedItem) -> None:
        channel = self.bot.get_channel(self.channel_id)
        if item.topic == "press_release":
            embed = self._create_press_release_embed(item.content)
            await channel.send(embed=embed)
```

### 3. Configuration Constants
```python
# config/channels.py
DISCORD_CHANNELS = {
    'press_releases': 1163373835886805013,
    'fi_shorts': 1175019650963222599,  
    'errors': 1162053416290361516,
}
```

## 5. Framework Benefits for Your Project

### Current Pain Points → Framework Solutions

| Pain Point | Current State | Framework Solution |
|------------|---------------|-------------------|
| **Adding new data source** | Modify main.py + create new module + manual task setup | Add YAML config entry + implement Fetcher/Parser |
| **Changing notification destination** | Modify each module's Discord code | Swap Sink in config |
| **Testing scrapers** | Run full Discord bot | Unit test Fetcher/Parser in isolation |
| **Retry/error handling** | Duplicated across modules | Centralized in infra layer |
| **Rate limiting** | Manual delays scattered | Generic rate limiter in HTTP infra |

### Long-term Architecture Vision
```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Discord Bot   │    │   Orchestrator   │    │   Config YAML   │
│                 │    │                  │    │                 │  
│ !steam command  │    │ Task Scheduler   │    │ Services/Sinks  │
│ !chart command  │────│ Circuit Breaker  │────│ Schedules       │
│ !short command  │    │ Retry Logic      │    │ Environment     │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                              │
                              ▼
┌────────────────────────────────────────────────────────────────┐
│                       Plugin Services                          │
├─────────────────┬─────────────────┬─────────────────────────────┤
│ FI Short Seller │ MFN WebSocket   │ Steam Top Games             │
│                 │                 │                             │
│ Fetcher────────▶│ Fetcher────────▶│ Fetcher ───────────────────▶│
│ Parser─────────▶│ Parser─────────▶│ Parser ────────────────────▶│
│ DatabaseSink───▶│ DiscordSink────▶│ DatabaseSink + DiscordSink─▶│
└─────────────────┴─────────────────┴─────────────────────────────┘
```

This framework would make your bot much more maintainable and extensible while preserving all existing functionality.


## Comment:
I've created a comprehensive analysis of how your current gaming-invest-bot project maps to the proposed framework. Here are the key findings:

Summary of Analysis
Well-Aligned Modules ✅
fi_blankning.py (8/10) - Already has clear fetch→parse→notify pattern
avanzaauth.py (8/10) - Good authentication service abstraction
database.py (7/10) - Decent sink pattern, just needs generalization
Mixed Concerns ⚠️
mfn.py, placera.py, psstore.py, ig.py - All mix fetching/parsing with direct Discord integration
Anti-patterns ❌
main.py - Monolithic orchestrator mixing Discord commands with task management
steam.py - Contains commands, scraping, database ops, and pipeline logic in one file
chart.py - Mixes API integration, charting, and UI presentation
Key Recommendations
Start with fi_blankning.py - It's closest to the framework pattern and would be the easiest win
Extract common infrastructure - HTTP retry logic, Discord sinks, WebSocket management
Split main.py - Separate Discord bot from background task orchestration
Configuration-driven approach - Move from hard-coded tasks to YAML service definitions
The analysis shows your project would benefit significantly from this framework, particularly in terms of maintainability, testability, and adding new data sources. The document includes specific code examples and a phased migration approach to minimize risk while maximizing benefit.