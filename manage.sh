#!/bin/bash

# Function to wait for service to be ready
wait_for_service() {
    local service=$1
    local url=$2
    local max_attempts=30
    local attempt=1
    
    echo "Waiting for $service to be ready..."
    while [ $attempt -le $max_attempts ]; do
        if curl -s $url > /dev/null; then
            echo "✅ $service is ready"
            return 0
        fi
        echo "Attempt $attempt/$max_attempts: $service not ready yet..."
        sleep 5
        attempt=$((attempt + 1))
    done
    echo "❌ $service failed to become ready"
    return 1
}

# Function to check if containers are healthy
check_health() {
    echo "Checking container health..."
    for container in postgres-source postgres-target kafka zookeeper debezium-connect; do
        status=$(docker-compose ps -q $container)
        if [ -z "$status" ]; then
            echo "❌ $container is not running"
            return 1
        else
            echo "✅ $container is running"
        fi
    done
    return 0
}

# Function to register Debezium connector
register_connector() {
    echo "Waiting for Debezium Connect to be ready..."
    if ! wait_for_service "Debezium Connect" "http://localhost:8083/connectors"; then
        echo "Failed to connect to Debezium Connect"
        return 1
    fi
    
    echo "Registering Debezium connector..."
    curl -i -X POST -H "Accept:application/json" -H "Content-Type:application/json" \
        http://localhost:8083/connectors/ -d @debezium-connector-config.json
    
    echo -e "\nChecking connector status..."
    sleep 5
    curl -s http://localhost:8083/connectors/orders-connector/status || echo "Failed to get connector status"
}

# Function to start the environment
start() {
    echo "Starting containers..."
    docker-compose up -d
    
    echo "Waiting for containers to be ready..."
    sleep 30
    
    if check_health; then
        echo "All containers are running. Registering connector..."
        register_connector
    else
        echo "Some containers failed to start. Check docker-compose logs for details."
        exit 1
    fi
}

# Function to stop the environment
stop() {
    echo "Stopping containers..."
    docker-compose down
}

# Function to show logs
logs() {
    docker-compose logs -f
}

# Function to show connector topics
list_topics() {
    echo "Kafka topics:"
    docker-compose exec kafka kafka-topics --list --bootstrap-server kafka:29092
}

# Function to check connector status
check_connector() {
    echo "Checking Debezium connector status..."
    curl -s http://localhost:8083/connectors/orders-connector/status || echo "Failed to get connector status"
}

# Main script
case "$1" in
    start)
        start
        ;;
    stop)
        stop
        ;;
    restart)
        stop
        start
        ;;
    status)
        check_health
        check_connector
        ;;
    logs)
        logs
        ;;
    topics)
        list_topics
        ;;
    connector)
        check_connector
        ;;
    *)
        echo "Usage: $0 {start|stop|restart|status|logs|topics|connector}"
        exit 1
        ;;
esac

exit 0