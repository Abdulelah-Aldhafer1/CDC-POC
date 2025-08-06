#!/bin/bash

# CDC Engagement Streaming Pipeline - Environment Validation Script
# This script validates that the environment is properly configured

set -e

echo "🔍 CDC Environment Validation"
echo "=============================="

# Check if .env file exists
if [ ! -f ".env" ]; then
    echo "❌ Error: .env file not found!"
    echo "   Run './scripts/setup-env.sh' to create a secure environment file."
    exit 1
fi

# Check .env file permissions
PERMS=$(stat -c "%a" .env 2>/dev/null || stat -f "%Lp" .env 2>/dev/null)
if [ "$PERMS" != "600" ]; then
    echo "⚠️  Warning: .env file permissions are $PERMS (should be 600)"
    echo "   Run 'chmod 600 .env' to secure the file."
else
    echo "✅ .env file permissions are secure (600)"
fi

# Load environment variables
source .env

# Validate required variables
echo ""
echo "📋 Validating Environment Variables:"

# Database configuration
if [ -z "$POSTGRES_PASSWORD" ]; then
    echo "❌ POSTGRES_PASSWORD is not set"
    EXIT_CODE=1
else
    echo "✅ POSTGRES_PASSWORD is configured"
fi

if [ -z "$POSTGRES_USER" ]; then
    echo "❌ POSTGRES_USER is not set"
    EXIT_CODE=1
else
    echo "✅ POSTGRES_USER is configured"
fi

# Redis configuration
if [ -z "$REDIS_PASSWORD" ]; then
    echo "❌ REDIS_PASSWORD is not set"
    EXIT_CODE=1
else
    echo "✅ REDIS_PASSWORD is configured"
fi

# Kafka configuration
if [ -z "$KAFKA_BOOTSTRAP_SERVERS" ]; then
    echo "❌ KAFKA_BOOTSTRAP_SERVERS is not set"
    EXIT_CODE=1
else
    echo "✅ KAFKA_BOOTSTRAP_SERVERS is configured"
fi

# BigQuery configuration
if [ "$BIGQUERY_PROJECT_ID" = "your-gcp-project-id" ]; then
    echo "⚠️  BIGQUERY_PROJECT_ID is using default value"
else
    echo "✅ BIGQUERY_PROJECT_ID is configured"
fi

# Data generation configuration
if [ -z "$GENERATION_RATE" ]; then
    echo "❌ GENERATION_RATE is not set"
    EXIT_CODE=1
else
    echo "✅ GENERATION_RATE is configured: $GENERATION_RATE"
fi

# Flink configuration
if [ -z "$FLINK_PARALLELISM" ]; then
    echo "❌ FLINK_PARALLELISM is not set"
    EXIT_CODE=1
else
    echo "✅ FLINK_PARALLELISM is configured: $FLINK_PARALLELISM"
fi

# Check for default passwords
echo ""
echo "🔐 Security Validation:"

if [ "$POSTGRES_PASSWORD" = "postgres" ] || [ "$POSTGRES_PASSWORD" = "your_secure_postgres_password_here" ]; then
    echo "❌ POSTGRES_PASSWORD is using default/insecure value"
    EXIT_CODE=1
else
    echo "✅ POSTGRES_PASSWORD is secure"
fi

if [ "$REDIS_PASSWORD" = "redis123" ] || [ "$REDIS_PASSWORD" = "your_secure_redis_password_here" ]; then
    echo "❌ REDIS_PASSWORD is using default/insecure value"
    EXIT_CODE=1
else
    echo "✅ REDIS_PASSWORD is secure"
fi

# Check if passwords are strong enough
if [ ${#POSTGRES_PASSWORD} -lt 12 ]; then
    echo "⚠️  POSTGRES_PASSWORD is shorter than recommended (12+ characters)"
fi

if [ ${#REDIS_PASSWORD} -lt 12 ]; then
    echo "⚠️  REDIS_PASSWORD is shorter than recommended (12+ characters)"
fi

echo ""
if [ "$EXIT_CODE" = "1" ]; then
    echo "❌ Environment validation failed!"
    echo "   Please fix the issues above and run this script again."
    exit 1
else
    echo "✅ Environment validation passed!"
    echo "   Your CDC pipeline is ready for secure deployment."
fi 