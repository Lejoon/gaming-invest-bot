"""
Database infrastructure with SQLite and async support.
"""

import asyncio
import logging
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import aiosqlite


logger = logging.getLogger(__name__)


class Database:
    """Async SQLite database wrapper."""
    
    def __init__(self, db_path: str = "scraper.db"):
        self.db_path = Path(db_path)
        self._connection: Optional[aiosqlite.Connection] = None

    async def connect(self) -> None:
        """Connect to the database and run migrations."""
        if self._connection:
            return
        
        self._connection = await aiosqlite.connect(self.db_path)
        self._connection.row_factory = aiosqlite.Row
        await self._run_migrations()

    async def close(self) -> None:
        """Close the database connection."""
        if self._connection:
            await self._connection.close()
            self._connection = None

    @asynccontextmanager
    async def transaction(self):
        """Context manager for database transactions."""
        if not self._connection:
            await self.connect()
        
        try:
            await self._connection.execute("BEGIN")
            yield self._connection
            await self._connection.commit()
        except Exception:
            await self._connection.rollback()
            raise

    async def execute(self, sql: str, params: Tuple[Any, ...] = ()) -> aiosqlite.Cursor:
        """Execute a SQL statement."""
        if not self._connection:
            await self.connect()
        return await self._connection.execute(sql, params)

    async def fetch_one(self, sql: str, params: Tuple[Any, ...] = ()) -> Optional[aiosqlite.Row]:
        """Fetch one row."""
        cursor = await self.execute(sql, params)
        return await cursor.fetchone()

    async def fetch_all(self, sql: str, params: Tuple[Any, ...] = ()) -> List[aiosqlite.Row]:
        """Fetch all rows."""
        cursor = await self.execute(sql, params)
        return await cursor.fetchall()

    async def upsert(
        self,
        table: str,
        data: Dict[str, Any],
        pk_columns: List[str],
    ) -> None:
        """Upsert data into a table."""
        columns = list(data.keys())
        placeholders = ", ".join("?" * len(columns))
        values = list(data.values())
        
        # Build the conflict resolution clause
        update_columns = [col for col in columns if col not in pk_columns]
        if update_columns:
            update_clause = ", ".join(f"{col} = excluded.{col}" for col in update_columns)
            conflict_clause = f"ON CONFLICT({', '.join(pk_columns)}) DO UPDATE SET {update_clause}"
        else:
            conflict_clause = f"ON CONFLICT({', '.join(pk_columns)}) DO NOTHING"
        
        sql = f"""
            INSERT INTO {table} ({', '.join(columns)})
            VALUES ({placeholders})
            {conflict_clause}
        """
        
        await self.execute(sql, tuple(values))
        if not self._connection.in_transaction:
            await self._connection.commit()

    async def _run_migrations(self) -> None:
        """Run database migrations."""
        await self._connection.execute("""
            CREATE TABLE IF NOT EXISTS migrations (
                version INTEGER PRIMARY KEY,
                applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Check current version
        cursor = await self._connection.execute("SELECT MAX(version) FROM migrations")
        result = await cursor.fetchone()
        current_version = result[0] if result[0] else 0
        
        migrations = [
            self._migration_001_initial_tables,
            self._migration_002_add_indexes,
        ]
        
        for i, migration in enumerate(migrations, 1):
            if i > current_version:
                logger.info(f"Running migration {i}")
                await migration()
                await self._connection.execute("INSERT INTO migrations (version) VALUES (?)", (i,))
        
        await self._connection.commit()

    async def _migration_001_initial_tables(self) -> None:
        """Create initial tables for FI short interest data."""
        await self._connection.execute("""
            CREATE TABLE IF NOT EXISTS short_positions (
                lei TEXT PRIMARY KEY,
                company_name TEXT NOT NULL,
                position_percent REAL NOT NULL,
                latest_position_date TEXT,
                timestamp TEXT NOT NULL
            )
        """)
        
        await self._connection.execute("""
            CREATE TABLE IF NOT EXISTS position_holders (
                entity_name TEXT NOT NULL,
                issuer_name TEXT NOT NULL,
                isin TEXT NOT NULL,
                position_percent REAL NOT NULL,
                position_date TEXT,
                timestamp TEXT NOT NULL,
                comment TEXT,
                PRIMARY KEY (entity_name, issuer_name, isin)
            )
        """)

    async def _migration_002_add_indexes(self) -> None:
        """Add indexes for better query performance."""
        await self._connection.execute("""
            CREATE INDEX IF NOT EXISTS idx_short_positions_company 
            ON short_positions(company_name)
        """)
        
        await self._connection.execute("""
            CREATE INDEX IF NOT EXISTS idx_position_holders_issuer 
            ON position_holders(issuer_name)
        """)
        
        await self._connection.execute("""
            CREATE INDEX IF NOT EXISTS idx_position_holders_timestamp 
            ON position_holders(timestamp)
        """)
