CREATE OR REPLACE FUNCTION check_multiple_tables_replication_status(
    p_table_list TEXT[]
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
AS $func$
DECLARE
    table_spec TEXT;
    schema_part TEXT;
    table_part TEXT;
BEGIN
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
    
    FOREACH table_spec IN ARRAY p_table_list
    LOOP
        schema_part := split_part(table_spec, '.', 1);
        table_part := split_part(table_spec, '.', 2);
        
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
    
    DROP TABLE IF EXISTS temp_replication_status;
END;
$func$;