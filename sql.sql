mplementation Queries
PostgreSQL Function for Batch Replication Status Check
sql-- Function to check if batches have completed replication for a given table
CREATE OR REPLACE FUNCTION check_batch_replication_status(
    p_schema_name TEXT,
    p_table_name TEXT
)
RETURNS TABLE (
    schema_name TEXT,
    table_name TEXT,
    batch_id BIGINT,
    slot_name TEXT,
    publication_name TEXT,
    slot_current_lsn PG_LSN,
    batch_completion_lsn PG_LSN,
    replication_complete BOOLEAN,
    lag_bytes BIGINT
)
LANGUAGE plpgsql
AS $
BEGIN
    RETURN QUERY
    WITH batch_info AS (
        -- Get recent completed batches for the specified table
        SELECT 
            bc.schema_name,
            bc.table_name,
            bc.batch_id,
            bc.completion_lsn::pg_lsn as batch_lsn,
            bc.completion_timestamp
        FROM batch_control bc
        WHERE bc.schema_name = p_schema_name 
        AND bc.table_name = p_table_name
        AND bc.status = 'COMPLETED'
        AND bc.completion_lsn IS NOT NULL
        AND bc.completion_timestamp >= NOW() - INTERVAL '24 hours'
    ),
    table_publications AS (
        -- Find publications that include this table
        SELECT DISTINCT
            pt.pubname,
            pt.schemaname,
            pt.tablename
        FROM pg_publication_tables pt
        WHERE pt.schemaname = p_schema_name
        AND pt.tablename = p_table_name
    ),
    slot_info AS (
        -- Find replication slots associated with publications for this table
        SELECT 
            rs.slot_name,
            rs.confirmed_flush_lsn,
            rs.active,
            -- Match slot to publication (customize this logic based on your naming convention)
            CASE 
                WHEN EXISTS (
                    SELECT 1 FROM table_publications tp 
                    WHERE rs.slot_name LIKE '%' || tp.pubname || '%'
                    OR tp.pubname LIKE '%' || REPLACE(rs.slot_name, '_slot', '') || '%'
                ) THEN (
                    SELECT tp.pubname 
                    FROM table_publications tp 
                    WHERE rs.slot_name LIKE '%' || tp.pubname || '%'
                    OR tp.pubname LIKE '%' || REPLACE(rs.slot_name, '_slot', '') || '%'
                    LIMIT 1
                )
                ELSE NULL
            END as matched_publication
        FROM pg_replication_slots rs
        WHERE rs.slot_type = 'logical'
        AND rs.active = true
    )
    SELECT 
        bi.schema_name::TEXT,
        bi.table_name::TEXT,
        bi.batch_id,
        si.slot_name::TEXT,
        COALESCE(si.matched_publication, 'unknown')::TEXT as publication_name,
        si.confirmed_flush_lsn,
        bi.batch_lsn,
        CASE 
            WHEN si.confirmed_flush_lsn >= bi.batch_lsn THEN true
            ELSE false
        END as replication_complete,
        CASE 
            WHEN si.confirmed_flush_lsn < bi.batch_lsn 
            THEN pg_wal_lsn_diff(bi.batch_lsn, si.confirmed_flush_lsn)
            ELSE 0
        END as lag_bytes
    FROM batch_info bi
    CROSS JOIN slot_info si
    WHERE si.matched_publication IS NOT NULL
    OR EXISTS (
        -- Fallback: if no publication match, find slot by naming pattern
        SELECT 1 FROM pg_replication_slots rs2
        WHERE rs2.slot_name LIKE '%' || p_table_name || '%'
        AND rs2.slot_name = si.slot_name
    )
    ORDER BY bi.batch_id DESC;
END;
$;

-- Alternative simpler function that returns one row per table (latest batch status)
CREATE OR REPLACE FUNCTION check_latest_batch_replication_status(
    p_schema_name TEXT,
    p_table_name TEXT
)
RETURNS TABLE (
    schema_name TEXT,
    table_name TEXT,
    latest_batch_id BIGINT,
    slot_name TEXT,
    publication_name TEXT,
    slot_current_lsn PG_LSN,
    batch_completion_lsn PG_LSN,
    replication_complete BOOLEAN,
    minutes_since_completion NUMERIC
)
LANGUAGE plpgsql
AS $
BEGIN
    RETURN QUERY
    WITH latest_batch AS (
        -- Get the most recent completed batch for this table
        SELECT 
            bc.schema_name,
            bc.table_name,
            bc.batch_id,
            bc.completion_lsn::pg_lsn as batch_lsn,
            bc.completion_timestamp,
            ROW_NUMBER() OVER (ORDER BY bc.batch_id DESC) as rn
        FROM batch_control bc
        WHERE bc.schema_name = p_schema_name 
        AND bc.table_name = p_table_name
        AND bc.status = 'COMPLETED'
        AND bc.completion_lsn IS NOT NULL
    ),
    table_publication AS (
        -- Find the publication for this table
        SELECT 
            pt.pubname,
            pt.schemaname,
            pt.tablename
        FROM pg_publication_tables pt
        WHERE pt.schemaname = p_schema_name
        AND pt.tablename = p_table_name
        LIMIT 1  -- Take first match if multiple publications
    ),
    relevant_slot AS (
        -- Find the replication slot for this table's publication
        SELECT 
            rs.slot_name,
            rs.confirmed_flush_lsn,
            rs.active,
            tp.pubname
        FROM pg_replication_slots rs
        CROSS JOIN table_publication tp
        WHERE rs.slot_type = 'logical'
        AND rs.active = true
        AND (
            rs.slot_name LIKE '%' || tp.pubname || '%'
            OR rs.slot_name LIKE '%' || p_table_name || '%'
            OR tp.pubname LIKE '%' || REPLACE(rs.slot_name, '_slot', '') || '%'
        )
        ORDER BY 
            CASE 
                WHEN rs.slot_name LIKE '%' || tp.pubname || '%' THEN 1
                WHEN rs.slot_name LIKE '%' || p_table_name || '%' THEN 2
                ELSE 3
            END
        LIMIT 1
    )
    SELECT 
        lb.schema_name::TEXT,
        lb.table_name::TEXT,
        lb.batch_id,
        rs.slot_name::TEXT,
        COALESCE(rs.pubname, 'unknown')::TEXT as publication_name,
        rs.confirmed_flush_lsn,
        lb.batch_lsn,
        CASE 
            WHEN rs.confirmed_flush_lsn >= lb.batch_lsn THEN true
            ELSE false
        END as replication_complete,
        ROUND(EXTRACT(EPOCH FROM (NOW() - lb.completion_timestamp))/60, 2) as minutes_since_completion
    FROM latest_batch lb
    CROSS JOIN relevant_slot rs
    WHERE lb.rn = 1;
END;
$;

-- Function to check multiple tables at once
CREATE OR REPLACE FUNCTION check_multiple_tables_replication_status(
    p_table_list TEXT[]  -- Array of 'schema.table' strings
)
RETURNS TABLE (
    schema_name TEXT,
    table_name TEXT,
    latest_batch_id BIGINT,
    slot_name TEXT,
    publication_name TEXT,
    replication_complete BOOLEAN,
    lag_bytes BIGINT,
    minutes_since_completion NUMERIC,
    health_status TEXT
)
LANGUAGE plpgsql
AS $
DECLARE
    table_spec TEXT;
    schema_part TEXT;
    table_part TEXT;
BEGIN
    -- Create temporary table to hold results
    CREATE TEMP TABLE IF NOT EXISTS temp_replication_status (
        schema_name TEXT,
        table_name TEXT,
        latest_batch_id BIGINT,
        slot_name TEXT,
        publication_name TEXT,
        replication_complete BOOLEAN,
        lag_bytes BIGINT,
        minutes_since_completion NUMERIC,
        health_status TEXT
    );
    
    -- Process each table in the input array
    FOREACH table_spec IN ARRAY p_table_list
    LOOP
        -- Split schema.table
        schema_part := split_part(table_spec, '.', 1);
        table_part := split_part(table_spec, '.', 2);
        
        -- Insert results for this table
        INSERT INTO temp_replication_status
        SELECT 
            r.schema_name,
            r.table_name,
            r.latest_batch_id,
            r.slot_name,
            r.publication_name,
            r.replication_complete,
            CASE 
                WHEN NOT r.replication_complete 
                THEN pg_wal_lsn_diff(r.batch_completion_lsn, r.slot_current_lsn)
                ELSE 0
            END as lag_bytes,
            r.minutes_since_completion,
            CASE 
                WHEN r.replication_complete THEN 'READY'
                WHEN r.minutes_since_completion > 60 THEN 'STUCK'
                WHEN r.minutes_since_completion > 30 THEN 'SLOW'
                ELSE 'REPLICATING'
            END as health_status
        FROM check_latest_batch_replication_status(schema_part, table_part) r;
    END LOOP;
    
    -- Return all results
    RETURN QUERY
    SELECT * FROM temp_replication_status
    ORDER BY 
        CASE health_status
            WHEN 'STUCK' THEN 1
            WHEN 'SLOW' THEN 2
            WHEN 'REPLICATING' THEN 3
            WHEN 'READY' THEN 4
        END,
        schema_name,
        table_name;
    
    -- Clean up
    DROP TABLE IF EXISTS temp_replication_status;
END;
$;
Usage Examples
sql-- Check replication status for a specific table
SELECT * FROM check_batch_replication_status('public', 'orders');

-- Check latest batch status for a table
SELECT * FROM check_latest_batch_replication_status('public', 'customers');

-- Check multiple tables at once
SELECT * FROM check_multiple_tables_replication_status(
    ARRAY['public.orders', 'public.customers', 'public.products', 'inventory.items']
);

-- Find all tables ready for reconciliation
SELECT 
    schema_name,
    table_name,
    latest_batch_id,
    slot_name,
    minutes_since_completion
FROM check_multiple_tables_replication_status(
    -- Get all tables from publications
    ARRAY(
        SELECT DISTINCT schemaname || '.' || tablename 
        FROM pg_publication_tables 
        WHERE pubname LIKE '%snowflake%'
    )
)
WHERE replication_complete = true
AND minutes_since_completion >= 5  -- 5 minute settle time
ORDER BY minutes_since_completion DESC;
Required Batch Control Table Structure
sql-- Expected structure of the batch_control table
CREATE TABLE IF NOT EXISTS batch_control (
    id BIGSERIAL PRIMARY KEY,
    schema_name VARCHAR(100) NOT NULL,
    table_name VARCHAR(100) NOT NULL,
    batch_id BIGINT NOT NULL,
    status VARCHAR(20) NOT NULL CHECK (status IN ('IN_PROGRESS', 'COMPLETED', 'FAILED')),
    start_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completion_timestamp TIMESTAMP,
    completion_lsn TEXT,  -- Store LSN as text, cast to pg_lsn in queries
    row_count INTEGER,
    error_message TEXT,
    
    UNIQUE(schema_name, table_name, batch_id)
);

CREATE INDEX idx_batch_control_completion 
ON batch_control(schema_name, table_name, status, completion_timestamp DESC);

CREATE INDEX idx_batch_control_lsn 
ON batch_control(completion_lsn) 
WHERE status = 'COMPLETED' AND completion_lsn IS NOT NULL;
System Catalog Queries Used by Functions
The functions internally use these PostgreSQL system catalog queries:
sql-- Publications and their tables
SELECT pubname, schemaname, tablename 
FROM pg_publication_tables;

-- Replication slots and their current positions
SELECT 
    slot_name, 
    slot_type, 
    active, 
    confirmed_flush_lsn,
    pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn) as lag_bytes
FROM pg_replication_slots 
WHERE slot_type = 'logical';

-- Current WAL position
SELECT pg_current_wal_lsn();
These functions provide a complete solution for checking batch replication status by combining your batch control data with PostgreSQL's system catalogs to determine replication completion status.