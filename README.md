# PostgreSQL CDC Replication System

This project implements Change Data Capture (CDC) between two PostgreSQL databases using Debezium. It includes data generation utilities, monitoring functions, and verification tools.

## System Architecture

- Source PostgreSQL database with logical replication enabled
- Target PostgreSQL database for receiving changes
- Debezium with Kafka for CDC
- Python utilities for data generation and verification
- SQL functions for monitoring replication status

## Prerequisites

- Docker and Docker Compose
- Python 3.7+
- PostgreSQL client tools
- curl (for Debezium API calls)

## Setup Instructions

1. Start the environment:
   ```bash
   chmod +x manage.sh
   ./manage.sh start
   ```

2. Verify all services are running:
   ```bash
   ./manage.sh status
   ```

3. Check Kafka topics:
   ```bash
   ./manage.sh topics
   ```

## Generating Test Data

Use the data generator to create and load test orders:

```bash
# Generate 100 orders (default batch size)
python data_generator.py

# Generate specific batch size
python data_generator.py --batch-size 500 --batch-id 1001
```

Configuration can be modified in `config.json`.

## Monitoring Replication

The system provides several ways to monitor replication:

1. SQL Functions:
   ```sql
   -- Check specific table
   SELECT * FROM check_batch_replication_status('public', 'orders');
   
   -- Check latest batch
   SELECT * FROM check_latest_batch_replication_status('public', 'orders');
   
   -- Check multiple tables
   SELECT * FROM check_multiple_tables_replication_status(
       ARRAY['public.orders', 'public.batch_control']
   );
   ```

2. Python Verification Script:
   ```bash
   python verify_replication.py --batch-id 1001 --timeout 300 --interval 10
   ```

## Component Details

### 1. Source Database
- Logical replication enabled
- Publication for orders and batch_control tables
- Monitoring functions installed
- Batch control table for tracking changes

### 2. Target Database
- Matching table structure
- Replication slot configured
- Monitoring functions installed

### 3. Data Generator
- Generates random order data
- Creates CSV feed files
- Tracks LSN for each batch
- Updates batch control table

### 4. Verification Tool
- Monitors replication progress
- Verifies data consistency
- Provides detailed status reporting
- Supports timeout and retry settings

## Batch Control Table

The `batch_control` table tracks:
- Batch ID
- Schema and table names
- Status (IN_PROGRESS, COMPLETED, FAILED)
- Start and completion timestamps
- LSN tracking
- Row counts

## Monitoring Functions

1. `check_batch_replication_status`:
   - Checks all batches for a table
   - Shows replication lag
   - Provides LSN tracking

2. `check_latest_batch_replication_status`:
   - Shows status of most recent batch
   - Quick health check
   - Completion time tracking

3. `check_multiple_tables_replication_status`:
   - Batch overview across tables
   - Health status indicators
   - Prioritized by replication state

## Troubleshooting

1. Check container status:
   ```bash
   ./manage.sh status
   ```

2. View logs:
   ```bash
   ./manage.sh logs
   ```

3. Common issues:
   - Debezium connector not running: Check connector status
   - Replication lag: Monitor LSN differences
   - Data inconsistency: Use verification tool

## File Structure

```
.
├── docker-compose.yml           # Container configuration
├── manage.sh                    # Management script
├── config.json                  # Configuration file
├── data_generator.py           # Data generation utility
├── verify_replication.py       # Verification tool
├── debezium-connector-config.json  # Debezium configuration
└── init-scripts/               # Database initialization
    ├── source/                 # Source database scripts
    └── target/                 # Target database scripts
```

## Development

To modify or extend the system:

1. Data Generation:
   - Modify `data_generator.py` for different data patterns
   - Update `config.json` for new parameters

2. Monitoring:
   - Add functions to SQL initialization scripts
   - Update verification tool for new checks

3. Infrastructure:
   - Modify `docker-compose.yml` for new services
   - Update `manage.sh` for new commands

## License

MIT License