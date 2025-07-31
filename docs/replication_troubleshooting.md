# PostgreSQL CDC Replication Troubleshooting Guide

This guide provides SQL queries to help diagnose replication issues across multiple replication slots and tables.

## 1. Check Replication Slot Status

```sql
-- Get status of all replication slots and their current positions
WITH slot_stats AS (
    SELECT 
        slot_name,
        active,
        restart_lsn,
        confirmed_flush_lsn,
        pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn) as lag_bytes
    FROM pg_replication_slots
    WHERE slot_type = 'logical'
)
SELECT 
    slot_name,
    active,
    restart_lsn,
    confirmed_flush_lsn,
    CASE 
        WHEN lag_bytes > 1024*1024 THEN ROUND(lag_bytes/1024.0/1024.0, 2) || ' MB'
        WHEN lag_bytes > 1024 THEN ROUND(lag_bytes/1024.0, 2) || ' KB'
        ELSE lag_bytes || ' bytes'
    END as lag,
    pg_current_wal_lsn() as current_wal_lsn
FROM slot_stats
ORDER BY lag_bytes DESC;
```

Example output:
```
  slot_name   | active | restart_lsn | confirmed_flush_lsn |    lag     | current_wal_lsn
--------------+--------+-------------+--------------------+------------+----------------
 orders_slot  | t      | 0/1A416B8  | 0/1A4EBD0         | 288 bytes  | 0/1A4F5F0
 invoices_slot| t      | 0/1A416B8  | 0/1A4EBD0         | 1.5 MB     | 0/1A4F5F0
```

## 2. Check Publication-Table Mappings

```sql
-- List all publications and their tables
WITH pub_tables AS (
    SELECT 
        p.pubname,
        p.pubowner::regrole as owner,
        p.puballtables,
        ARRAY_AGG(pt.tablename) as tables
    FROM pg_publication p
    LEFT JOIN pg_publication_tables pt 
        ON p.pubname = pt.pubname
    GROUP BY p.pubname, p.pubowner, p.puballtables
)
SELECT 
    pubname,
    owner,
    puballtables as all_tables,
    tables as published_tables
FROM pub_tables;
```

## 3. Check Replication Status Across All Tables

```sql
-- Get replication status for all tables in batch_control
WITH recent_batches AS (
    SELECT DISTINCT ON (schema_name, table_name)
        schema_name,
        table_name,
        batch_id,
        completion_lsn::pg_lsn as batch_lsn,
        completion_timestamp
    FROM batch_control
    WHERE status = 'COMPLETED'
    AND completion_timestamp >= NOW() - INTERVAL '24 hours'
    ORDER BY schema_name, table_name, batch_id DESC
),
slot_info AS (
    SELECT 
        slot_name,
        confirmed_flush_lsn,
        active
    FROM pg_replication_slots
    WHERE slot_type = 'logical'
    AND active = true
)
SELECT 
    rb.schema_name,
    rb.table_name,
    rb.batch_id as latest_batch,
    s.slot_name,
    s.confirmed_flush_lsn,
    rb.batch_lsn,
    s.confirmed_flush_lsn >= rb.batch_lsn as replication_complete,
    CASE 
        WHEN s.confirmed_flush_lsn < rb.batch_lsn 
        THEN pg_wal_lsn_diff(rb.batch_lsn, s.confirmed_flush_lsn)
        ELSE 0
    END as lag_bytes
FROM recent_batches rb
CROSS JOIN slot_info s
ORDER BY lag_bytes DESC;
```

## 4. Compare Source and Target Data Per Table

```sql
-- Create a temporary function to compare table counts
CREATE OR REPLACE FUNCTION compare_table_counts(p_schema text, p_table text)
RETURNS TABLE (
    schema_name text,
    table_name text,
    source_count bigint,
    target_count bigint,
    mismatch boolean
) AS $$
DECLARE
    source_sql text;
    target_sql text;
    source_count bigint;
    target_count bigint;
BEGIN
    -- Get source count
    source_sql := format('SELECT COUNT(*) FROM %I.%I', p_schema, p_table);
    EXECUTE source_sql INTO source_count;
    
    -- Get target count (adjust connection details as needed)
    target_sql := format('SELECT COUNT(*) FROM dblink(''dbname=targetdb'', ''SELECT COUNT(*) FROM %I.%I'') AS t(count bigint)', p_schema, p_table);
    EXECUTE target_sql INTO target_count;
    
    RETURN QUERY SELECT 
        p_schema,
        p_table,
        source_count,
        target_count,
        source_count != target_count;
END;
$$ LANGUAGE plpgsql;

-- Use the function to check all tables
SELECT * FROM (
    SELECT DISTINCT schema_name, table_name 
    FROM batch_control
) t,
LATERAL compare_table_counts(t.schema_name, t.table_name)
WHERE mismatch = true;
```

## 5. Monitor Replication Lag Trends

```sql
-- Track lag over time for all slots
SELECT 
    slot_name,
    completion_timestamp,
    pg_wal_lsn_diff(
        completion_lsn::pg_lsn,
        (SELECT confirmed_flush_lsn 
         FROM pg_replication_slots 
         WHERE slot_name = bc.slot_name)
    ) as historical_lag_bytes
FROM batch_control bc
WHERE completion_timestamp >= NOW() - INTERVAL '1 hour'
ORDER BY completion_timestamp DESC;
```

## Common Issues and Solutions

1. **Multiple Slots Out of Sync**
   - Check each slot's lag independently
   - Look for patterns in lagging slots (same tables, similar timing)
   - Verify consumer connections for each slot

2. **Publication-Table Mismatches**
   - Verify all tables are included in correct publications
   - Check for typos in schema/table names
   - Confirm publication ownership and permissions

3. **Cross-Table Dependencies**
   - Monitor related tables together
   - Check foreign key relationships
   - Verify transaction boundaries across tables

4. **Resource Contention**
   - Monitor system resources per slot
   - Check for blocking queries
   - Review WAL generation rate vs. consumption rate

5. **Slot Management**
   - Regular cleanup of inactive slots
   - Monitor disk space used by retained WAL
   - Track restart_lsn progression