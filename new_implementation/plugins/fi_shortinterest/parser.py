"""
FI Short Interest Parsers - Parse ODS files into structured data.
"""

import io
import logging
from typing import Dict, List

import pandas as pd

from core.interfaces import Parser
from core.models import RawItem, ParsedItem


logger = logging.getLogger(__name__)


def _read_ods(raw: bytes, column_map: Dict[int, str]) -> pd.DataFrame:
    """Read ODS file from bytes and rename columns."""
    bio = io.BytesIO(raw)
    df = pd.read_excel(bio, sheet_name="Blad1", skiprows=5, engine="odf")
    df.rename(columns={df.columns[i]: new for i, new in column_map.items()}, inplace=True)
    return df


class FiAggParser(Parser):
    """Parser for FI aggregate short interest data."""
    
    name = "FiAggParser"
    
    _cols = {
        0: "company_name", 
        1: "lei", 
        2: "position_percent", 
        3: "latest_position_date"
    }

    async def parse(self, item: RawItem) -> List[ParsedItem]:
        """Parse aggregate short interest data."""
        if not item.source.endswith("agg"):
            return []
        
        try:
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
            
            logger.info(f"Parsed {len(out)} aggregate records")
            return out
            
        except Exception as e:
            logger.error(f"Failed to parse aggregate data: {e}")
            return []


class FiActParser(Parser):
    """Parser for FI current position data."""
    
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
        """Parse current position data."""
        if not item.source.endswith("act"):
            return []
        
        try:
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
            
            logger.info(f"Parsed {len(out)} position records")
            return out
            
        except Exception as e:
            logger.error(f"Failed to parse position data: {e}")
            return []
