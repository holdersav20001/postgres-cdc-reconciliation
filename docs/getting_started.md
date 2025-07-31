# PostgreSQL CDC Replication Testing Guide

This guide explains how to run and test the PostgreSQL CDC replication system using Debezium.

## Prerequisites

- Docker and Docker Compose
- Git
- PostgreSQL client (psql)

## Setup

1. Clone the repository:
```bash
git clone <repository-url>
cd postgres-cdc-reconciliation
```

2. Start the environment:
```bash
docker-compose up -d
```

This will start:
- Source PostgreSQL (port 5432)
- Target PostgreSQL (port 5433)
- Kafka
- Zookeeper
- Kafka Connect with Debezium

## Verify Services

Check all services are running:
```bash
docker-compose ps
```

All services should show status as "Up".

## Configure Replication

1. Create Debezium source connector:
```bash
curl -X POST http://localhost:8083/connectors -H "Content-Type: application/json" -d @debezium-connector-config.json
```

2. Create JDBC sink connector:
```bash
curl -X POST http://localhost:8083/connectors -H "Content-Type: application/json" -d @jdbc-sink-connector-config.json
```

3. Verify connectors are running:
```bash
curl http://localhost:8083/connectors
curl http://localhost:8083/connectors/orders-connector/status
curl http://localhost:8083/connectors/jdbc-sink-connector/status
```

## Test Replication

1. Insert test data into source database:
```bash
docker-compose exec postgres-source psql -U sourceuser -d sourcedb -c "
INSERT INTO batch_control (
    schema_name, table_name, batch_id, status, 
    completion_timestamp, completion_lsn, row_count
) VALUES (
    'public', 'orders', 1, 'COMPLETED', 
    NOW(), (SELECT pg_current_wal_lsn()::TEXT), 1
);

INSERT INTO orders (
    order_id, customer_id, amount, timestamp, batch_id
) VALUES (
    1, 100, 50.00, NOW(), 1
);"
```

2. Check replication status:
```bash
docker-compose exec postgres-source psql -U sourceuser -d sourcedb -c "
SELECT * FROM check_batch_replication_status('public', 'orders');"
```

Expected output shows:
- replication_complete: true
- lag_bytes: 0

3. Verify data in target:
```bash
docker-compose exec postgres-target psql -U targetuser -d targetdb -c "
SELECT * FROM batch_control ORDER BY batch_id;
SELECT * FROM orders ORDER BY order_id;"
```

## Test Replication Lag Detection

1. Stop the JDBC sink connector:
```bash
curl -X PUT http://localhost:8083/connectors/jdbc-sink-connector/pause
```

2. Insert new data:
```bash
docker-compose exec postgres-source psql -U sourceuser -d sourcedb -c "
INSERT INTO batch_control (
    schema_name, table_name, batch_id, status,
    completion_timestamp, completion_lsn, row_count
) VALUES (
    'public', 'orders', 2, 'COMPLETED',
    NOW(), (SELECT pg_current_wal_lsn()::TEXT), 1
);

INSERT INTO orders (
    order_id, customer_id, amount, timestamp, batch_id
) VALUES (
    2, 101, 75.00, NOW(), 2
);"
```

3. Check replication status:
```bash
docker-compose exec postgres-source psql -U sourceuser -d sourcedb -c "
SELECT * FROM check_batch_replication_status('public', 'orders');"
```

Expected output shows:
- replication_complete: false
- lag_bytes: > 0

4. Resume replication:
```bash
curl -X PUT http://localhost:8083/connectors/jdbc-sink-connector/resume
```

5. Verify lag resolves:
```bash
docker-compose exec postgres-source psql -U sourceuser -d sourcedb -c "
SELECT * FROM check_batch_replication_status('public', 'orders');"
```

## Monitor Multiple Tables

1. Check status across all tables:
```bash
docker-compose exec postgres-source psql -U sourceuser -d sourcedb -c "
SELECT * FROM check_multiple_tables_replication_status();"
```

2. View publication-table mappings:
```bash
docker-compose exec postgres-source psql -U sourceuser -d sourcedb -c "
SELECT * FROM pg_publication_tables;"
```

## Troubleshooting

If issues occur:

1. Check connector logs:
```bash
docker-compose logs kafka-connect
```

2. View replication slots:
```bash
docker-compose exec postgres-source psql -U sourceuser -d sourcedb -c "
SELECT * FROM pg_replication_slots;"
```

3. See detailed troubleshooting guide:
```
docs/replication_troubleshooting.md
```

## Cleanup

Stop and remove all containers:
```bash
docker-compose down -v
```

This removes all containers and volumes, requiring full setup for next run.