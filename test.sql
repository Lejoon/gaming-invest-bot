------------------------------------------------------------------
-- migrate_position_holders_inplace.sql
--  Creates / populates position_holders_history from positionholders
------------------------------------------------------------------

------------------------------------------------------------------
-- 1. History table (create only if it isn't there yet)
------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS position_holders_history (
    entity_name        TEXT NOT NULL,
    issuer_name        TEXT NOT NULL,
    isin               TEXT NOT NULL,
    position_percent   REAL,        -- value *after* the change
    position_date      DATE,
    event_timestamp    DATETIME,    -- snapshot timestamp (copied from positionholders.timestamp)
    old_pct            REAL,
    new_pct            REAL,
    PRIMARY KEY (entity_name, issuer_name, isin, event_timestamp)
);

------------------------------------------------------------------
-- 2. Build the diff rows directly out of positionholders
------------------------------------------------------------------
WITH
/* a)  Keep just one row per holder/issuer/isin **per run** (latest wins) */
dedup AS (
    SELECT
        entity_name,
        issuer_name,
        isin,
        position_percent,
        position_date,
        timestamp          AS event_timestamp
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                   PARTITION BY entity_name, issuer_name, isin, timestamp
                   ORDER BY ROWID DESC
               ) AS rn
        FROM positionholders
    )
    WHERE rn = 1
),

/* b)  Bring in the immediately-previous % and date for that trio */
lagged AS (
    SELECT
        entity_name,
        issuer_name,
        isin,
        position_percent    AS new_pct,
        position_date,
        event_timestamp,
        LAG(position_percent) OVER (
             PARTITION BY entity_name, issuer_name, isin
             ORDER BY event_timestamp
        )                    AS old_pct,
        LAG(position_date) OVER (
             PARTITION BY entity_name, issuer_name, isin
             ORDER BY event_timestamp
        )                    AS old_date
    FROM dedup
),

/* c)  First-ever rows (old_pct NULL)   OR   meaningful changes */
diff AS (
    SELECT
        entity_name,
        issuer_name,
        isin,
        COALESCE(old_pct, 0.0) AS old_pct,
        new_pct,
        position_date,
        event_timestamp
    FROM lagged
    WHERE old_pct IS NULL                             -- brand-new holder/issuer/isin
       OR ABS(new_pct - old_pct) > 0.00001            -- % really changed
       OR position_date <> COALESCE(old_date,'')      -- or only the date moved
)

------------------------------------------------------------------
-- 3. Insert, skipping anything we've already migrated before
------------------------------------------------------------------
INSERT OR IGNORE INTO position_holders_history
      (entity_name, issuer_name, isin,
       position_percent, position_date,
       event_timestamp, old_pct, new_pct)
SELECT
      entity_name,
      issuer_name,
      isin,
      new_pct              AS position_percent,
      position_date,
      event_timestamp,
      old_pct,
      new_pct
FROM diff;

------------------------------------------------------------------
-- 4. Done
------------------------------------------------------------------
