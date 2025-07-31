# Docker Configuration for PostgreSQL CDC Setup

## Docker Compose Configuration

```yaml
version: '3.8'

services:
  # Source PostgreSQL Database
  postgres-source:
    image: postgres:15
    environment:
      POSTGRES_DB: sourcedb
      POSTGRES_USER: sourceuser
      POSTGRES_PASSWORD: sourcepass
    ports:
      - "5432:5432"
    volumes:
      - postgres-source-data:/var/lib/postgresql/data
    command: 
      - "postgres"
      - "-c"
      - "wal_level=logical"
    networks:
      - cdc-network

  # Target PostgreSQL Database
  postgres-target:
    image: postgres:15
    environment:
      POSTGRES_DB: targetdb
      POSTGRES_USER: targetuser
      POSTGRES_PASSWORD: targetpass
    ports:
      - "5433:5432"
    volumes:
      - postgres-target-data:/var/lib/postgresql/data
    networks:
      - cdc-network

  # Zookeeper for Kafka
  zookeeper:
    image: confluentinc/cp-zookeeper:7.3.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    networks:
      - cdc-network

  # Kafka Broker
  kafka:
    image: confluentinc/cp-kafka:7.3.0
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
    networks:
      - cdc-network

  # Debezium Connect
  debezium-connect:
    image: debezium/connect:2.3
    depends_on:
      - kafka
      - postgres-source
    ports:
      - "8083:8083"
    environment:
      GROUP_ID: 1
      BOOTSTRAP_SERVERS: kafka:29092
      CONFIG_STORAGE_TOPIC: connect_configs
      OFFSET_STORAGE_TOPIC: connect_offsets
      STATUS_STORAGE_TOPIC: connect_statuses
    networks:
      - cdc-network

volumes:
  postgres-source-data:
  postgres-target-data:

networks:
  cdc-network:
    driver: bridge
```

## Debezium Connector Configuration

After the containers are running, we'll need to configure the Debezium PostgreSQL connector:

```json
{
  "name": "orders-connector",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "tasks.max": "1",
    "database.hostname": "postgres-source",
    "database.port": "5432",
    "database.user": "sourceuser",
    "database.password": "sourcepass",
    "database.dbname": "sourcedb",
    "database.server.name": "postgres-source",
    "table.include.list": "public.orders,public.batch_control",
    "plugin.name": "pgoutput",
    "publication.name": "orders_pub",
    "slot.name": "orders_slot"
  }
}
```

## Initial Database Setup Scripts

### Source Database

```sql
-- Enable logical replication
ALTER SYSTEM SET wal_level = logical;

-- Create orders table
CREATE TABLE public.orders (
    order_id SERIAL PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    amount DECIMAL(10,2) NOT NULL,
    timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Create publication
CREATE PUBLICATION orders_pub FOR TABLE public.orders, public.batch_control;
```

### Target Database

```sql
-- Create matching orders table
CREATE TABLE public.orders (
    order_id INTEGER PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    amount DECIMAL(10,2) NOT NULL,
    timestamp TIMESTAMP NOT NULL
);
```

## Next Steps

1. Create the Docker Compose file
2. Start the containers: `docker-compose up -d`
3. Wait for all services to be healthy
4. Apply database setup scripts to source and target databases
5. Configure Debezium connector using the REST API
6. Implement the monitoring functions from sql.sql
7. Create the data generation utility