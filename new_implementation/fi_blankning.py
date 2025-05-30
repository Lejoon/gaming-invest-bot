"""
fi_blankning.py  –  Financial Supervisory Authority (Sweden) short-selling scraper
==================================================================================

This module plugs into the generic Fetcher→Parser→Sink framework:

    FiFetcher ─▶ FiAggParser ─┐
                              ├─▶ FiDiffParser ─▶ (DatabaseSink | DiscordSink | …)
    (agg.ods / act.ods)       │
    FiFetcher ─▶ FiActParser ─┘

Only the **Fetcher** knows about URLs, only the **Parsers** know the
ODS layout, and only the **Sinks** know how to persist / publish.

Author : Lukas  ·  2025-05-27
"""

from __future__ import annotations

import asyncio
import io
import os
from datetime import datetime, timedelta
from typing import Any, AsyncIterator, Dict, List, Tuple

import aiohttp
import asyncpg
import pandas as pd
from bs4 import BeautifulSoup
from pydantic import BaseModel, Field

# --------------------------------------------------------------------------- #
# 0.  Core abstractions (copied locally so the file is self-contained)        #
# --------------------------------------------------------------------------- #

class RawItem(BaseModel):
    source: str
    payload: bytes
    fetched_at: datetime = Field(default_factory=datetime.utcnow)


class ParsedItem(BaseModel):
    topic: str
    content: Dict[str, Any]
    discovered_at: datetime = Field(default_factory=datetime.utcnow)


class Fetcher:               # (simplified – no ABC for brevity)
    name: str

    async def fetch(self) -> AsyncIterator[RawItem]:            # pragma: no cover
        raise NotImplementedError


class Parser:
    name: str

    async def parse(self, item: RawItem) -> List[ParsedItem]:   # pragma: no cover
        raise NotImplementedError


class Sink:
    name: str

    async def handle(self, item: ParsedItem) -> None:           # pragma: no cover
        raise NotImplementedError


# --------------------------------------------------------------------------- #
# 1.  Fetcher – gets timestamp, downloads ODS if changed                      #
# --------------------------------------------------------------------------- #

class FiFetcher(Fetcher):
    name = "FiFetcher"

    URL_TS  = "https://www.fi.se/sv/vara-register/blankningsregistret/"
    URL_AGG = "https://www.fi.se/sv/vara-register/blankningsregistret/GetBlankningsregisterAggregat/"
    URL_ACT = "https://www.fi.se/sv/vara-register/blankningsregistret/GetAktuellFile/"
    SLEEP   = 15 * 60                                  # 15 min polling

    def __init__(self, session: aiohttp.ClientSession | None = None):
        self._external_session = session
        self._last_seen: str | None = None

    # ---------- helpers --------------------------------------------------- #

    async def _get_html(self, url: str, session: aiohttp.ClientSession) -> str:
        for attempt in range(5):
            try:
                async with session.get(url, timeout=30) as r:
                    r.raise_for_status()
                    return await r.text()
            except Exception:
                await asyncio.sleep(min(2 ** attempt, 60))
        raise RuntimeError(f"Failed to GET {url}")

    async def _get_bytes(self, url: str, session: aiohttp.ClientSession) -> bytes:
        for attempt in range(5):
            try:
                async with session.get(url, timeout=60) as r:
                    r.raise_for_status()
                    return await r.read()
            except Exception:
                await asyncio.sleep(min(2 ** attempt, 60))
        raise RuntimeError(f"Failed to GET {url}")

    async def _poll_timestamp(self, session: aiohttp.ClientSession) -> str | None:
        html = await self._get_html(self.URL_TS, session)
        soup = BeautifulSoup(html, "lxml")
        tag = soup.find("p", string=lambda t: "Listan uppdaterades:" in t if t else False)
        if not tag:
            return None
        return tag.text.split(": ")[1].strip()

    # ---------- async generator ------------------------------------------- #

    async def fetch(self) -> AsyncIterator[RawItem]:
        own_session = None
        session = self._external_session or aiohttp.ClientSession()
        if not self._external_session:
            own_session = session                 # close later

        try:
            while True:
                ts = await self._poll_timestamp(session)
                if ts and ts != "0001-01-01 00:00" and ts != self._last_seen:
                    # download both files
                    agg_bytes = await self._get_bytes(self.URL_AGG, session)
                    act_bytes = await self._get_bytes(self.URL_ACT, session)

                    fetched_at = datetime.utcnow()
                    self._last_seen = ts

                    yield RawItem(source="fi.short.agg", payload=agg_bytes, fetched_at=fetched_at)
                    yield RawItem(source="fi.short.act", payload=act_bytes, fetched_at=fetched_at)

                await asyncio.sleep(self.SLEEP)
        finally:
            if own_session:
                await own_session.close()


# --------------------------------------------------------------------------- #
# 2.  Parsers – ODS → canonical dicts                                         #
# --------------------------------------------------------------------------- #

def _read_ods(raw: bytes, column_map: Dict[int, str]) -> pd.DataFrame:
    bio = io.BytesIO(raw)
    df = pd.read_excel(bio, sheet_name="Blad1", skiprows=5, engine="odf")
    df.rename(columns={df.columns[i]: new for i, new in column_map.items()}, inplace=True)
    return df


class FiAggParser(Parser):
    name = "FiAggParser"

    _cols = {0: "company_name", 1: "lei", 2: "position_percent", 3: "latest_position_date"}

    async def parse(self, item: RawItem) -> List[ParsedItem]:
        if not item.source.endswith("agg"):
            return []
        df = _read_ods(item.payload, self._cols)
        df["company_name"] = df["company_name"].str.strip()
        df["timestamp"] = item.fetched_at.isoformat()

        out: List[ParsedItem] = []
        for rec in df.to_dict("records"):
            out.append(
                ParsedItem(
                    topic="fi.short.aggregate",
                    content=rec,
                    discovered_at=item.fetched_at,
                )
            )
        return out


class FiActParser(Parser):
    name = "FiActParser"

    _cols = {
        0: "entity_name",
        1: "issuer_name",
        2: "isin",
        3: "position_percent",
        4: "position_date",
        5: "comment",
    }

    async def parse(self, item: RawItem) -> List[ParsedItem]:
        if not item.source.endswith("act"):
            return []
        df = _read_ods(item.payload, self._cols)
        df["issuer_name"] = df["issuer_name"].str.strip()
        df["entity_name"] = df["entity_name"].str.strip()
        df["timestamp"] = item.fetched_at.isoformat()

        out: List[ParsedItem] = []
        for rec in df.to_dict("records"):
            out.append(
                ParsedItem(
                    topic="fi.short.positions",
                    content=rec,
                    discovered_at=item.fetched_at,
                )
            )
        return out


# --------------------------------------------------------------------------- #
# 3.  Diff-Parser – emit only if changed                                      #
# --------------------------------------------------------------------------- #

class FiDiffParser(Parser):
    """
    Stateless diffing would need DB reads; instead we keep a *tiny* in-memory
    cache keyed by PK and rely on fetcher's 15 min cadence.
    """

    name = "FiDiffParser"

    def __init__(self):
        self._last_agg: Dict[str, float] = {}                       # lei → pct
        self._last_act: Dict[Tuple[str, str, str], float] = {}      # (entity,issuer,isin)

    async def parse(self, item: RawItem) -> List[ParsedItem]:       # never called
        raise RuntimeError("FiDiffParser consumes ParsedItem, not RawItem")

    async def diff(self, p_item: ParsedItem) -> List[ParsedItem]:
        if p_item.topic == "fi.short.aggregate":
            lei = p_item.content["lei"]
            pct = p_item.content["position_percent"]
            if self._last_agg.get(lei) != pct:
                self._last_agg[lei] = pct
                return [p_item.copy(update={"topic": "fi.short.aggregate.diff"})]
            return []

        if p_item.topic == "fi.short.positions":
            key = (
                p_item.content["entity_name"],
                p_item.content["issuer_name"],
                p_item.content["isin"],
            )
            pct = p_item.content["position_percent"]
            if self._last_act.get(key) != pct:
                self._last_act[key] = pct
                return [p_item.copy(update={"topic": "fi.short.positions.diff"})]
            return []

        return []


# --------------------------------------------------------------------------- #
# 4.  Simple Database sink (asyncpg UPSERT)                                   #
# --------------------------------------------------------------------------- #

class DatabaseSink(Sink):
    name = "DatabaseSink"

    _TABLE_MAP = {
        "fi.short.aggregate": {
            "table": "short_positions",
            "pk": ["lei"],
            "cols": [
                "lei",
                "company_name",
                "position_percent",
                "latest_position_date",
                "timestamp",
            ],
        },
        "fi.short.aggregate.diff": {  # same table / cols
            "table": "short_positions",
            "pk": ["lei"],
            "cols": [
                "lei",
                "company_name",
                "position_percent",
                "latest_position_date",
                "timestamp",
            ],
        },
        "fi.short.positions": {
            "table": "position_holders",
            "pk": ["entity_name", "issuer_name", "isin"],
            "cols": [
                "entity_name",
                "issuer_name",
                "isin",
                "position_percent",
                "position_date",
                "timestamp",
            ],
        },
        "fi.short.positions.diff": {  # ditto
            "table": "position_holders",
            "pk": ["entity_name", "issuer_name", "isin"],
            "cols": [
                "entity_name",
                "issuer_name",
                "isin",
                "position_percent",
                "position_date",
                "timestamp",
            ],
        },
    }

    def __init__(self, dsn: str):
        self._dsn = dsn
        self._pool: asyncpg.Pool | None = None

    async def _ensure_pool(self):
        if not self._pool:
            self._pool = await asyncpg.create_pool(self._dsn, min_size=1, max_size=4)

    async def handle(self, item: ParsedItem) -> None:
        if item.topic not in self._TABLE_MAP:
            return
        await self._ensure_pool()
        m = self._TABLE_MAP[item.topic]
        cols = m["cols"]
        table = m["table"]
        pk = m["pk"]

        values = [item.content.get(c) for c in cols]
        placeholders = ", ".join(f"${i}" for i in range(1, len(values) + 1))
        update_set = ", ".join(f"{c}=EXCLUDED.{c}" for c in cols if c not in pk)
        sql = f"""
            INSERT INTO {table} ({', '.join(cols)})
            VALUES ({placeholders})
            ON CONFLICT ({', '.join(pk)}) DO UPDATE
            SET {update_set};
        """
        async with self._pool.acquire() as conn:
            await conn.execute(sql, *values)


# --------------------------------------------------------------------------- #
# 5.  Pipeline glue for *this* file (no orchestrator here)                    #
# --------------------------------------------------------------------------- #

async def _run_pipeline(
    fetcher: FiFetcher,
    parsers: List[Parser],
    diff_parser: FiDiffParser,
    sinks: List[Sink],
):
    """
    Minimal glue: Fetcher → Parsers → Diff → Sinks
    (The global orchestrator would normally manage this.)
    """
    async for raw in fetcher.fetch():
        for p in parsers:
            p_items = await p.parse(raw)
            for pi in p_items:
                # diff filtering
                diffed = await diff_parser.diff(pi)
                for d_item in diffed:
                    for s in sinks:
                        await s.handle(d_item)


# --------------------------------------------------------------------------- #
# 6.  Public API: call `run()` from your bot / service entrypoint             #
# --------------------------------------------------------------------------- #

async def run(
    pg_dsn: str,
    external_session: aiohttp.ClientSession | None = None,
) -> None:
    """
    Kick off the FI short-interest pipeline.
    Parameters
    ----------
    pg_dsn : str
        Postgres DSN, e.g. ``"postgresql://user:pass@localhost/dbname"``.
    external_session : aiohttp.ClientSession | None
        Optionally provide one to reuse (e.g. your bot already has a session).
    """
    fetcher = FiFetcher(session=external_session)
    parsers: list[Parser] = [FiAggParser(), FiActParser()]
    diff_parser = FiDiffParser()
    sinks: list[Sink] = [DatabaseSink(pg_dsn)]

    await _run_pipeline(fetcher, parsers, diff_parser, sinks)


# --------------------------------------------------------------------------- #
# 7.  CLI helper for local testing                                            #
# --------------------------------------------------------------------------- #

if __name__ == "__main__":
    import argparse
    import logging

    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    argp = argparse.ArgumentParser(description="Run FI short-selling scraper standalone")
    argp.add_argument("--pg", required=True, help="PostgreSQL DSN")
    args = argp.parse_args()

    asyncio.run(run(args.pg))
