#!/bin/bash

# CDC Engagement Streaming Pipeline - Environment Setup Script
# This script helps you create a secure .env file for production deployment

set -e

echo "🔐 CDC Engagement Streaming Pipeline - Secure Environment Setup"
echo "================================================================"

# Check if .env already exists
if [ -f ".env" ]; then
    echo "⚠️  Warning: .env file already exists!"
    read -p "Do you want to overwrite it? (y/N): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "Setup cancelled. Your existing .env file is preserved."
        exit 0
    fi
fi

# Generate secure passwords
echo "🔑 Generating secure passwords..."
POSTGRES_PASSWORD=$(openssl rand -base64 32 | tr -d "=+/" | cut -c1-25)
REDIS_PASSWORD=$(openssl rand -base64 32 | tr -d "=+/" | cut -c1-25)

# Create .env file from template
echo "📝 Creating .env file with secure credentials..."

cat > .env << EOF
# CDC Engagement Streaming Pipeline - Environment Configuration
# Generated on $(date)
# WARNING: Keep this file secure and never commit it to version control

# =============================================================================
# DATABASE CONFIGURATION
# =============================================================================
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_DB=engagement_db
POSTGRES_USER=postgres
POSTGRES_PASSWORD=${POSTGRES_PASSWORD}

# =============================================================================
# REDIS CONFIGURATION
# =============================================================================
REDIS_HOST=redis
REDIS_PORT=6379
REDIS_PASSWORD=${REDIS_PASSWORD}
REDIS_DB=0

# =============================================================================
# KAFKA CONFIGURATION
# =============================================================================
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_SCHEMA_REGISTRY_URL=http://schema-registry:8081
KAFKA_CONSUMER_GROUP=engagement-streaming-job

# =============================================================================
# BIGQUERY CONFIGURATION
# =============================================================================
BIGQUERY_PROJECT_ID=your-gcp-project-id
BIGQUERY_DATASET=analytics
BIGQUERY_TABLE=engagement_events
BIGQUERY_CREDENTIALS_FILE=/path/to/your/service-account-key.json

# =============================================================================
# DATA GENERATION CONFIGURATION
# =============================================================================
GENERATION_RATE=200000
BURST_MODE=false
BURST_COUNT=100000

# =============================================================================
# FLINK CONFIGURATION
# =============================================================================
FLINK_CHECKPOINT_INTERVAL=30000
FLINK_PARALLELISM=4
FLINK_TASKMANAGER_MEMORY=4096m

# =============================================================================
# DEBEZIUM CONFIGURATION
# =============================================================================
DEBEZIUM_SLOT_NAME=debezium_slot
DEBEZIUM_PUBLICATION_NAME=dbz_publication
DEBEZIUM_PLUGIN_NAME=pgoutput

# =============================================================================
# SECURITY CONFIGURATION
# =============================================================================
# Enable SSL/TLS for database connections (optional for production)
POSTGRES_SSL_MODE=prefer
POSTGRES_SSL_CERT=/path/to/client-cert.pem
POSTGRES_SSL_KEY=/path/to/client-key.pem
POSTGRES_SSL_CA=/path/to/ca-cert.pem

# =============================================================================
# MONITORING CONFIGURATION
# =============================================================================
LOG_LEVEL=INFO
METRICS_ENABLED=true
HEALTH_CHECK_ENABLED=true
EOF

# Set proper permissions
chmod 600 .env

echo "✅ .env file created successfully!"
echo ""
echo "🔐 Generated secure credentials:"
echo "   PostgreSQL Password: ${POSTGRES_PASSWORD}"
echo "   Redis Password: ${REDIS_PASSWORD}"
echo ""
echo "⚠️  IMPORTANT SECURITY NOTES:"
echo "   1. The .env file has been created with restricted permissions (600)"
echo "   2. Generated passwords are cryptographically secure"
echo "   3. Never commit .env file to version control"
echo "   4. Store passwords securely in production (use secret management)"
echo ""
echo "📋 Next steps:"
echo "   1. Review and customize the .env file if needed"
echo "   2. Update BigQuery configuration for your GCP project"
echo "   3. Configure SSL certificates if needed for production"
echo "   4. Run: docker-compose up -d"
echo ""
echo "🚀 Ready to deploy securely!" 