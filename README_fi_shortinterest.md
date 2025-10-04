## FI Short Interest Data (Sweden) – Architecture & Schema Guide

This document explains how Swedish Financial Supervisory Authority (Finansinspektionen, FI) short interest data is ingested, modeled, and updated inside the SQLite database (`steam_top_games.db`) used by this project. Although the database name reflects other features (Steam / PS scraping), the same file also stores tables for equity short interest monitoring.

---
### Source Overview
Two endpoints from FI are polled (see `fi_blankning.py`):

1. Aggregate register (`Blankningsregisteraggregat.ods`)
   - Provides per issuer (company) the aggregated short position (sum of positions ≥ 0.5%).
2. Current positions file (`AktuellaPositioner.ods`)
   - Provides each reporting entity's individual net short position ≥ 0.5% for an issuer.
3. A webpage timestamp (`https://www.fi.se/.../blankningsregistret/`)
   - Contains the last published update line: `Listan uppdaterades: YYYY-MM-DD HH:MM`.

The ingestion loop (`update_fi_from_web`) periodically (every 15 minutes by default) checks if the remote published timestamp changed. Only when it changes are new .ods files downloaded and processed. This prevents redundant inserts.

---
### Update Flow Summary
1. Fetch current published timestamp (retry until not the placeholder `0001-01-01 00:00`).
2. If unchanged from locally stored `last_known_timestamp.txt`, sleep until next cycle.
3. Download both .ods files.
4. Parse them with `pandas.read_excel(..., engine="odf")` after skipping header rows.
5. Normalize & rename columns.
6. Load existing historical rows from `ShortPositions` and `PositionHolders`.
7. Compute diffs (new / changed / dropped positions) and insert only those.
8. Emit Discord embeds for a configured set of tracked companies (`TRACKED_COMPANIES`).

All inserted rows for a polling cycle share the same `timestamp` value – the FI publication time, NOT the local ingestion time.

---
### Table: ShortPositions
Schema (from `database.py`):
```
CREATE TABLE IF NOT EXISTS ShortPositions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp DATETIME,              -- FI list publication timestamp (YYYY-MM-DD HH:MM)
    company_name TEXT NOT NULL,      -- Issuer official name (trimmed)
    lei TEXT,                        -- Legal Entity Identifier for the issuer
    position_percent REAL,           -- Aggregated % of net short positions ≥ 0.5%
    latest_position_date TEXT        -- Date of the latest position event contributing to aggregate
);
```

Population details:
* Derived from the aggregate file (`Blankningsregisteraggregat.ods`).
* Column mapping performed in `read_aggregate_data()`.
* Duplicate handling: before diffing, both old and new frames are sorted by `timestamp` and deduplicated on `(lei, company_name)` taking the last occurrence.
* Diff logic (`update_database_diff`):
  - New issuers → inserted with full row.
  - Existing issuers → inserted only when `position_percent` changed.
  - No explicit “zeroing” of removed companies (historical disappearance is not backfilled with a 0 row).

Semantic notes:
* `position_percent` is stored as the raw value from FI (e.g. `3.15` meaning 3.15%).
* Plotting / time series logic divides by 100 for display scaling (`plot_timeseries`).
* Aggregated figure reflects only disclosed positions (each ≥ 0.5%); total actual market shorting could be higher.

Typical query for latest value:
```sql
SELECT company_name, position_percent, timestamp
FROM ShortPositions
WHERE LOWER(company_name) LIKE '%embracer%'
ORDER BY timestamp DESC
LIMIT 1;
```

Time series construction logic (see `create_timeseries`):
1. Query last 3 months bounded by now and earliest daily anchor.
2. Resample to daily frequency taking last reported value each day.
3. Forward-fill gaps (days without new FI publication keep prior value).

---
### Table: PositionHolders
Schema:
```
CREATE TABLE IF NOT EXISTS PositionHolders (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_name TEXT,          -- Reporting short seller entity
    issuer_name TEXT,          -- Issuer (company) name (trimmed)
    isin TEXT,                 -- Security ISIN
    position_percent REAL,     -- Individual disclosed net short % (≥ 0.5% or 0.0 marker)
    position_date TEXT,        -- Date of that entity's position (from file)
    timestamp TEXT             -- FI list publication timestamp (cycle anchor)
);
```

Population details:
* Derived from the current positions file (`AktuellaPositioner.ods`).
* Column mapping performed in `read_current_data()`.
* Dedup key: `(entity_name, issuer_name, isin)` after sorting by `timestamp`.
* Diff logic (`update_position_holders`):
  - New positions (new key tuple) inserted with provided %.
  - Changed positions (same key, different `position_percent`) inserted as new row with updated %.
  - Dropped positions (present before, absent now) get a synthesized row setting `position_percent = 0.0` (only if prior percent ≠ 0.0 OR the previous timestamp is >= current fetch timestamp guard). This records cessation without deleting history.
* All rows in a cycle carry `timestamp = fetched_timestamp`.

Semantic notes:
* `position_percent` thresholds: FI discloses positions at or above 0.5%. If an entity disappears from the current file, it is explicitly recorded as `0.0` in our model for continuity.
* Consumers should treat `0.0` as “position dropped below disclosure threshold or closed.”
* `position_date` may lag `timestamp` (it’s the effective date of that position event within FI, not the publication moment).

Example: retrieve latest disclosed holders for a company:
```sql
WITH latest_pub AS (
  SELECT MAX(timestamp) AS ts FROM PositionHolders WHERE issuer_name = 'Embracer Group AB'
)
SELECT entity_name, position_percent, position_date, timestamp
FROM PositionHolders, latest_pub
WHERE issuer_name = 'Embracer Group AB' AND timestamp = latest_pub.ts
ORDER BY position_percent DESC;
```

Detecting position closures:
```sql
SELECT * FROM PositionHolders
WHERE issuer_name = 'Paradox Interactive AB (publ)'
  AND position_percent = 0.0
ORDER BY timestamp DESC
LIMIT 10;
```

---
### Discord Notification Logic
When new or changed aggregate rows involve a company in `TRACKED_COMPANIES`, an embed is built summarizing:
* New aggregate short % and delta versus prior value.
* Any holder-level changes (new, changed, or dropped ≥ 0.5% positions) formatted per entity.
* Link constructed with the issuer `lei` into FI’s issuer page.

If the bot context is absent (e.g., manual/test usage), descriptions are printed to stdout instead.

---
### Data Quality & Edge Cases
| Case | Handling |
|------|----------|
| FI timestamp placeholder `0001-01-01 00:00` | Retries every 30s until a real timestamp appears. |
| Duplicate issuer or holder entries within a single file | Normalized by deduping on key columns before diffing. |
| Missing `.ods` columns or parse errors | Exception routed to `report_error_to_channel`. |
| Removed holder | Insert synthetic 0.0 row to mark drop. |
| Removed issuer (aggregate) | Currently no explicit 0.0 marker (could be an enhancement). |
| Time zone | FI timestamp used verbatim (assumed local Swedish time); no conversion performed. |

Potential improvements:
1. Record ingestion time separately (e.g., `ingested_at`).
2. Add 0.0 synthetic rows for removed issuers for symmetry with holders.
3. Enforce NOT NULL on key columns for data integrity.
4. Add indices: `(company_name, timestamp)`, `(issuer_name, entity_name, timestamp)` for faster queries.
5. Normalize issuers & entities into dimension tables to reduce repetition.
6. Store numeric dates as ISO strings consistently (`YYYY-MM-DD`).
7. Add a view that reconstructs current live state per issuer (latest non-zero per holder + aggregate).

---
### Example Analytical Queries

Daily aggregate evolution for a tracked issuer (forward-filled externally):
```sql
SELECT substr(timestamp, 1, 10) AS day,
       MAX(timestamp) AS last_pub_ts,
       AVG(position_percent) AS avg_reported_percent,
       MAX(position_percent) AS last_reported_percent
FROM ShortPositions
WHERE company_name = 'Stillfront Group AB (publ)'
GROUP BY day
ORDER BY day;
```

Holder churn (entries vs exits) over last 90 days:
```sql
WITH base AS (
  SELECT entity_name, issuer_name, position_percent, timestamp,
         LAG(position_percent) OVER (PARTITION BY entity_name, issuer_name ORDER BY timestamp) AS prev_pct
  FROM PositionHolders
  WHERE timestamp >= datetime('now', '-90 days')
)
SELECT issuer_name,
       COUNT(CASE WHEN prev_pct IS NULL AND position_percent > 0 THEN 1 END) AS new_entries,
       COUNT(CASE WHEN prev_pct > 0 AND position_percent = 0 THEN 1 END)     AS exits
FROM base
GROUP BY issuer_name
ORDER BY exits DESC;
```

Reconstruct current live holder state (latest non-zero per holder):
```sql
WITH ranked AS (
  SELECT *, ROW_NUMBER() OVER (
           PARTITION BY entity_name, issuer_name
           ORDER BY timestamp DESC) AS rn
  FROM PositionHolders
  WHERE position_percent > 0
)
SELECT issuer_name, entity_name, position_percent, position_date, timestamp AS last_seen
FROM ranked WHERE rn = 1
ORDER BY issuer_name, position_percent DESC;
```

---
### Integration Touchpoints
Code References:
* `fi_blankning.py`
  - `update_fi_from_web` – main loop.
  - `read_aggregate_data`, `read_current_data` – parsing.
  - `update_database_diff` – aggregate diff insertion.
  - `update_position_holders` – per-entity diff insertion (& zero markers).
  - `create_timeseries`, `plot_timeseries` – chart utility for command responses.
* `database.py`
  - Table schemas (`SHORT_POSITIONS_SCHEMA`, `POSITION_HOLDERS_SCHEMA`).
  - `insert_bulk_data` – generic batch insert including FI tables.

Discord command example (`short_command` in `fi_blankning.py`) performs:
1. Fuzzy issuer lookup in `ShortPositions`.
2. Time series construction and chart generation.
3. Sends textual latest % plus chart image.

---
### Operational Notes
* Retry logic with exponential backoff wraps network fetches (`@aiohttp_retry`).
* If FI site temporarily omits a timestamp, loop waits 30 seconds instead of full 15-minute cycle.
* Errors are directed to configured error + public channels; public placeholder message indicates temporary unavailability.
* Removal of downloaded `.ods` files after parsing keeps workspace clean.

---
### Glossary
| Term | Meaning |
|------|---------|
| Aggregate (issuer) short % | Sum of all disclosed net short positions ≥ 0.5% for that issuer. |
| Holder position | Individual reporting entity’s disclosed % (≥ 0.5%). |
| Publication timestamp | The FI list update time extracted from the webpage. |
| Position date | Date associated with a specific position inside FI’s file (may precede publication). |

---
### Suggested Next Enhancements (Backlog)
* Add foreign key constraints (if refactoring into dimension tables).
* Implement archival / pruning strategy if database grows large.
* Add automated tests simulating diff scenarios (add, change, drop) to prevent regressions.
* Provide a lightweight API endpoint on top of the SQLite for external consumption.

---
If you need clarifications or want to extend functionality (e.g., issuer normalization or dashboarding), open an issue or continue the discussion in the project channel.
