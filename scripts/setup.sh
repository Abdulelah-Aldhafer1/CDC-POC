#!/bin/bash

# Setup script for the CDC Engagement Streaming Pipeline
# This script initializes the entire system and starts all services

set -e  # Exit on any error

echo "🚀 Starting CDC Engagement Streaming Pipeline Setup..."

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Function to print colored output
print_status() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check prerequisites
check_prerequisites() {
    print_status "Checking prerequisites..."
    
    if ! command -v docker &> /dev/null; then
        print_error "Docker is not installed. Please install Docker first."
        exit 1
    fi
    
    if ! command -v docker-compose &> /dev/null; then
        print_error "Docker Compose is not installed. Please install Docker Compose first."
        exit 1
    fi
    
    if ! command -v mvn &> /dev/null; then
        print_warning "Maven is not installed. Flink job compilation will be skipped."
    fi
    
    print_status "Prerequisites check completed."
}

# Create necessary directories
create_directories() {
    print_status "Creating necessary directories..."
    
    mkdir -p monitoring/grafana/dashboards
    mkdir -p monitoring/grafana/datasources
    mkdir -p flink/lib
    mkdir -p logs
    
    print_status "Directories created."
}

# Build Flink streaming job
build_flink_job() {
    if command -v mvn &> /dev/null; then
        print_status "Building Flink streaming job..."
        
        cd flink-streaming-job
        mvn clean package -DskipTests
        
        # Copy JAR to Flink lib directory
        cp target/engagement-streaming-*.jar ../flink/lib/
        cd ..
        
        print_status "Flink job built and copied to lib directory."
    else
        print_warning "Skipping Flink job build - Maven not available."
    fi
}

# Start infrastructure services
start_infrastructure() {
    print_status "Starting infrastructure services..."
    
    # Start core services first
    docker-compose up -d postgres zookeeper kafka schema-registry redis
    
    # Wait for services to be ready
    print_status "Waiting for services to be ready..."
    sleep 30
    
    # Check if services are healthy
    check_service_health
}

# Check service health
check_service_health() {
    print_status "Checking service health..."
    
    # Check PostgreSQL
    until docker-compose exec postgres pg_isready -U postgres; do
        print_warning "Waiting for PostgreSQL to be ready..."
        sleep 5
    done
    
    # Check Kafka
    until docker-compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list &> /dev/null; do
        print_warning "Waiting for Kafka to be ready..."
        sleep 5
    done
    
    # Check Redis
    until docker-compose exec redis redis-cli -a redis123 ping | grep PONG; do
        print_warning "Waiting for Redis to be ready..."
        sleep 5
    done
    
    print_status "All core services are healthy."
}

# Setup Kafka topics
setup_kafka_topics() {
    print_status "Setting up Kafka topics..."
    
    # Create topics with proper partitioning
    docker-compose exec kafka kafka-topics --create \
        --bootstrap-server localhost:9092 \
        --topic cdc.engagement_db.public.engagement_events \
        --partitions 16 \
        --replication-factor 1 \
        --if-not-exists
    
    docker-compose exec kafka kafka-topics --create \
        --bootstrap-server localhost:9092 \
        --topic cdc.engagement_db.public.content \
        --partitions 4 \
        --replication-factor 1 \
        --if-not-exists
    
    print_status "Kafka topics created."
}

# Start CDC and processing services
start_processing_services() {
    print_status "Starting CDC and processing services..."
    
    # Start Kafka Connect
    docker-compose up -d connect
    sleep 20
    
    # Start Flink
    docker-compose up -d jobmanager taskmanager
    sleep 15
    
    print_status "Processing services started."
}

# Configure Debezium connector
setup_debezium_connector() {
    print_status "Setting up Debezium CDC connector..."
    
    # Wait for Kafka Connect to be ready
    until curl -f http://localhost:8083/connectors; do
        print_warning "Waiting for Kafka Connect to be ready..."
        sleep 10
    done
    
    # Create the PostgreSQL connector
    curl -X POST \
        -H "Content-Type: application/json" \
        -d @debezium/postgres-connector.json \
        http://localhost:8083/connectors
    
    print_status "Debezium connector configured."
}


# Start data generator
start_data_generator() {
    print_status "Starting data generator..."
    
    docker-compose up -d data-generator
    
    print_status "Data generator started - generating 1M records per 5 minutes."
}

# Deploy Flink job
deploy_flink_job() {
    if [ -f "flink/lib/engagement-streaming-*.jar" ]; then
        print_status "Deploying Flink streaming job..."
        
        # Wait for Flink to be ready
        until curl -f http://localhost:8080/overview; do
            print_warning "Waiting for Flink to be ready..."
            sleep 10
        done
        
        # Submit the job
        docker-compose exec jobmanager flink run \
            /opt/flink/lib/engagement-streaming-*.jar
        
        print_status "Flink job deployed."
    else
        print_warning "Flink job JAR not found - skipping deployment."
    fi
}

# Verify system health
verify_system() {
    print_status "Verifying system health..."
    
    # Check all services are running
    docker-compose ps
    
    # Check Kafka topics
    print_status "Kafka topics:"
    docker-compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list
    
    # Check Debezium connector status
    print_status "Debezium connector status:"
    curl -s http://localhost:8083/connectors/postgres-engagement-connector/status | jq .
    
    # Check Flink jobs
    print_status "Flink jobs:"
    curl -s http://localhost:8080/jobs | jq .
    
    print_status "System health check completed."
}

# Print access information
print_access_info() {
    print_status "🎉 System setup completed successfully!"
    echo ""
    echo "📊 Access Points:"
    echo "  • Flink Dashboard:    http://localhost:8080"
    echo "  • Kafka Connect:     http://localhost:8083"
    echo ""
    echo "📈 Processing:"
    echo "  • Data generation:   1M records per 5 minutes"
    echo "  • Redis latency:     <5 seconds SLA"
    echo "  • BigQuery:          30-minute partitions"
    echo ""
    echo "🔧 Management Commands:"
    echo "  • View logs:         docker-compose logs -f [service]"
    echo "  • Scale services:    docker-compose up -d --scale taskmanager=8"
    echo "  • Stop system:       docker-compose down"
    echo ""
}

# Cleanup function
cleanup() {
    print_error "Setup interrupted. Cleaning up..."
    docker-compose down --remove-orphans
    exit 1
}

# Trap cleanup on script interruption
trap cleanup INT TERM

# Main execution flow
main() {
    check_prerequisites
    create_directories
    build_flink_job
    start_infrastructure
    setup_kafka_topics
    start_processing_services
    setup_debezium_connector
    start_data_generator
    deploy_flink_job
    verify_system
    print_access_info
}

# Run main function
main "$@"

print_status "Setup completed. System is now running!"
print_status "Monitor the logs with: docker-compose logs -f"