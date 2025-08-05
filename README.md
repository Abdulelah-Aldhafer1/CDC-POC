# Real-Time CDC Engagement Streaming Pipeline

A robust, scalable big data streaming solution that processes user engagement events from PostgreSQL to multiple destinations in real-time using Change Data Capture (CDC).

## 🏗️ Architecture Overview

![Real-Time Data Processing Pipeline Timeline](arc.png)


### Key Components

- **PostgreSQL**: Source database with engagement events and content metadata
- **Debezium**: Change Data Capture for real-time event streaming
- **Apache Kafka**: Distributed message streaming (16 partitions)
- **Apache Flink**: Stream processing with exactly-once guarantees
- **Redis**: Real-time engagement metrics (<5 second SLA)
- **BigQuery**: Analytics data warehouse (30-minute partitions)

## 📊 Data Flow

### Source Schema
```sql
-- Content metadata
CREATE TABLE content (
    id UUID PRIMARY KEY,
    slug TEXT UNIQUE NOT NULL,
    title TEXT NOT NULL,
    content_type TEXT CHECK (content_type IN ('podcast', 'newsletter', 'video')),
    length_seconds INTEGER,
    publish_ts TIMESTAMPTZ NOT NULL
);

-- Raw engagement events
CREATE TABLE engagement_events (
    id BIGSERIAL PRIMARY KEY,
    content_id UUID REFERENCES content(id),
    user_id UUID,
    event_type TEXT CHECK (event_type IN ('play', 'pause', 'finish', 'click')),
    event_ts TIMESTAMPTZ NOT NULL,
    duration_ms INTEGER,
    device TEXT,
    raw_payload JSONB
);
```

### Transformations
1. **Join**: engagement_events + content metadata
2. **Calculate**: `engagement_seconds = duration_ms / 1000`
3. **Calculate**: `engagement_pct = (engagement_seconds / length_seconds) * 100`
4. **Enrich**: Add content metadata fields

### Output Destinations

#### 1. Redis (Real-time)
- **SLA**: <5 seconds
- **Purpose**: Top engagement leaderboards, real-time dashboards
- **Structure**: Sorted sets, hash maps with TTL

#### 2. BigQuery (Analytics)
- **Partitioning**: 30-minute time partitions
- **Clustering**: By content_type, event_type
- **Purpose**: Historical analytics, reporting

#### 3. External System (Future)
- **Format**: HTTP POST with configurable payload
- **Retry**: Exponential backoff with circuit breaker

## 🚀 Quick Start

### Prerequisites
- Docker & Docker Compose
- Maven (for Flink job compilation)
- 8GB+ RAM recommended

### 1. Clone and Setup
```bash
git clone <repository>
cd CDC
./scripts/setup.sh
```

### 2. Verify Deployment
```bash
# Check all services
docker-compose ps

# View logs
docker-compose logs -f flink-streaming-job

# Check data generation
docker-compose logs -f data-generator
```

### 3. Access Dashboards
- **Flink Dashboard**: http://localhost:8080

## 📈 Performance & Scale

### Throughput
- **Target**: 1M records per 5 minutes (3,333 records/second)
- **Tested**: Up to 288M records/day
- **Scaling**: Horizontal via Kafka partitions + Flink parallelism

### Latency SLA
- **Redis**: <5 seconds end-to-end
- **BigQuery**: Near real-time (30-second micro-batches)
- **Processing**: <100ms per event

### Resource Requirements
```yaml
# Production Sizing
Flink TaskManagers: 4x (4GB RAM each)
Kafka Partitions: 16
Redis: 2GB RAM
PostgreSQL: 4 vCPU, 8GB RAM
```

## 🔧 Configuration

### Scaling Up
```bash
# Increase Flink parallelism
docker-compose up -d --scale taskmanager=8

# Increase data generation rate
docker-compose exec data-generator \
  env GENERATION_RATE=400000 python generator.py
```

### Environment Variables
```bash
# Data Generation
GENERATION_RATE=200000      # Records per minute
BURST_MODE=false           # Continuous vs burst mode

# Database
POSTGRES_HOST=postgres
POSTGRES_DB=engagement_db

# Redis
REDIS_HOST=redis
REDIS_PASSWORD=redis123

# BigQuery (configure in Flink job)
BIGQUERY_PROJECT_ID=your-project
BIGQUERY_DATASET=analytics
```

## 🛡️ Reliability Features

### Exactly-Once Processing
- Flink checkpointing (30-second intervals)
- Kafka transactional producers
- Idempotent sink operations

### Error Handling
- Dead letter queues for failed records
- Circuit breakers for external systems
- Automatic retries with exponential backoff
- Side outputs for monitoring errors

### High Availability
- Kafka replication (RF=3 in production)
- Flink HA with shared storage
- PostgreSQL streaming replication
- Redis clustering support

## 📊 System Monitoring

### Key Performance Indicators  
- **Redis Latency**: <5 seconds SLA
- **Processing Rate**: 3,333+ records/second  
- **Data Volume**: 1M records per 5 minutes
- **Exactly-Once**: Guaranteed via Flink checkpointing

## 🧪 Testing

### Load Testing
```bash
# Burst test with 100k events
docker-compose exec data-generator \
  env BURST_MODE=true BURST_COUNT=100000 python generator.py

# Sustained load test
docker-compose exec data-generator \
  env GENERATION_RATE=500000 python generator.py
```

### Data Validation
```bash
# Check Redis data
docker-compose exec redis redis-cli -a redis123 ZRANGE top_engagement:10min 0 -1 WITHSCORES

# Query BigQuery (if configured)
bq query "SELECT content_type, AVG(engagement_pct) FROM analytics.engagement_events 
          WHERE DATE(event_ts) = CURRENT_DATE() GROUP BY content_type"
```

## 🔄 Operational Procedures

### Backfill Historical Data
```bash
# Stop real-time processing
docker-compose exec jobmanager flink cancel <job-id>

# Switch to batch mode and reprocess
# (Implementation in Flink job supports both CDC and JDBC sources)
```

### Schema Evolution
1. Update PostgreSQL schema
2. Update Flink job data models
3. Deploy new version with savepoint
4. Debezium automatically handles schema changes

### Disaster Recovery
1. PostgreSQL: Point-in-time recovery from WAL
2. Kafka: Topic replication and backup
3. Flink: Restart from last checkpoint
4. Redis: Rebuild from stream replay

## 🏭 Production Considerations

### Security
- PostgreSQL SSL connections
- Kafka SASL authentication
- Redis AUTH passwords
- BigQuery service account permissions

### Cost Optimization
- BigQuery partitioning reduces scan costs
- Redis TTL prevents unbounded growth
- Kafka log compaction for content topics
- Auto-scaling based on load

### Maintenance
- Regular checkpoint cleanup
- Kafka log retention policies
- PostgreSQL WAL archiving
- Monitoring data retention

## 🚀 Productionizing with Confluent Cloud

For enterprise-grade production deployment, I recommend migrating to **Confluent Cloud** - the fully managed Apache Kafka service that provides enterprise features, global scalability, and operational simplicity.

### Why Confluent Cloud for This Project?

#### 1. **Managed Apache Kafka**
- **Auto-scaling**: Automatically scales Kafka clusters based on throughput demands
- **Multi-zone deployment**: Built-in high availability across availability zones
- **Managed upgrades**: Zero-downtime Kafka version upgrades
- **Global deployment**: Multi-region clusters for disaster recovery

#### 2. **Confluent Schema Registry**
- **Schema evolution**: Safe schema changes with backward/forward compatibility
- **Data governance**: Centralized schema management for engagement events
- **Validation**: Runtime schema validation prevents data corruption
- **Integration**: Native integration with Debezium and Flink

#### 3. **Confluent Connect (Managed Connectors)**
- **Debezium PostgreSQL**: Managed CDC connector with automatic failover
- **BigQuery Sink**: Native connector with exactly-once delivery
- **Redis Sink**: Optimized connector for real-time metrics
- **Monitoring**: Built-in connector health monitoring and alerting

#### 4. **Confluent Control Center**
- **Real-time monitoring**: Live dashboard for Kafka cluster health
- **Topic management**: Visual topic creation, configuration, and monitoring
- **Consumer lag tracking**: Monitor Flink job consumption rates
- **Performance metrics**: Throughput, latency, and error rate monitoring

#### 5. **Security & Compliance**
- **RBAC**: Role-based access control for team members
- **Encryption**: End-to-end encryption (at-rest and in-transit)
- **Audit logs**: Comprehensive audit trail for compliance
- **VPC peering**: Secure network connectivity to your infrastructure

#### 6. **Confluent Flink SQL**
- **Managed Flink**: Serverless Flink processing with auto-scaling
- **SQL interface**: Write streaming queries in SQL instead of Java/Scala
- **Built-in connectors**: Native support for Kafka, BigQuery, Redis
- **Monitoring**: Integrated monitoring and alerting for Flink jobs


