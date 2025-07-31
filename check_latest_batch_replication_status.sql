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
AS $func$
BEGIN
    RETURN QUERY
    WITH latest_batch AS (
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
        SELECT 
            pt.pubname,
            pt.schemaname,
            pt.tablename
        FROM pg_publication_tables pt
        WHERE pt.schemaname = p_schema_name
        AND pt.tablename = p_table_name
        LIMIT 1
    ),
    relevant_slot AS (
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
$func$;