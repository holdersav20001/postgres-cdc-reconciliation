-- Create orders table
CREATE TABLE public.orders (
    order_id SERIAL PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    amount DECIMAL(10,2) NOT NULL,
    timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    batch_id BIGINT NOT NULL
);

-- Create batch control table
CREATE TABLE public.batch_control (
    id BIGSERIAL PRIMARY KEY,
    schema_name VARCHAR(100) NOT NULL,
    table_name VARCHAR(100) NOT NULL,
    batch_id BIGINT NOT NULL,
    status VARCHAR(20) NOT NULL CHECK (status IN ('IN_PROGRESS', 'COMPLETED', 'FAILED')),
    start_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completion_timestamp TIMESTAMP,
    completion_lsn TEXT,
    row_count INTEGER,
    error_message TEXT,
    
    UNIQUE(schema_name, table_name, batch_id)
);

-- Create indexes for batch control table
CREATE INDEX idx_batch_control_completion 
ON batch_control(schema_name, table_name, status, completion_timestamp DESC);

CREATE INDEX idx_batch_control_lsn 
ON batch_control(completion_lsn) 
WHERE status = 'COMPLETED' AND completion_lsn IS NOT NULL;

-- Create publication for CDC
CREATE PUBLICATION orders_pub FOR TABLE public.orders, public.batch_control;

-- Create functions for monitoring replication status
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
AS $function$
BEGIN
    RETURN QUERY
    WITH batch_info AS (
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
        SELECT DISTINCT
            pt.pubname,
            pt.schemaname,
            pt.tablename
        FROM pg_publication_tables pt
        WHERE pt.schemaname = p_schema_name
        AND pt.tablename = p_table_name
    ),
    slot_info AS (
        SELECT 
            rs.slot_name,
            rs.confirmed_flush_lsn,
            rs.active,
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
        SELECT 1 FROM pg_replication_slots rs2
        WHERE rs2.slot_name LIKE '%' || p_table_name || '%'
        AND rs2.slot_name = si.slot_name
    )
    ORDER BY bi.batch_id DESC;
END;
$function$;