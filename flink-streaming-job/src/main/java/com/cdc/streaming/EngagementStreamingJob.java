package com.cdc.streaming;

import com.cdc.streaming.models.*;
import com.cdc.streaming.sinks.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Map;

/**
 * Real-time CDC engagement streaming job
 * Processes 1M records per 5 minutes with <5 second Redis latency SLA
 * 
 * Architecture:
 * PostgreSQL (CDC) -> Kafka -> Flink -> Redis (real-time) + BigQuery (analytics)
 */
public class EngagementStreamingJob {
    private static final Logger LOG = LoggerFactory.getLogger(EngagementStreamingJob.class);
    
    // Configuration from environment variables
    private static String getEnvOrDefault(String key, String defaultValue) {
        return System.getenv(key) != null ? System.getenv(key) : defaultValue;
    }
    
    private static final String KAFKA_BOOTSTRAP_SERVERS = getEnvOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
    private static final String ENGAGEMENT_EVENTS_TOPIC = "cdc.engagement_db.public.engagement_events";
    private static final String CONTENT_TOPIC = "cdc.engagement_db.public.content";
    private static final String CONSUMER_GROUP = getEnvOrDefault("KAFKA_CONSUMER_GROUP", "engagement-streaming-job");
    
    // Redis configuration from environment
    private static final String REDIS_HOST = getEnvOrDefault("REDIS_HOST", "localhost");
    private static final int REDIS_PORT = Integer.parseInt(getEnvOrDefault("REDIS_PORT", "6379"));
    private static final String REDIS_PASSWORD = getEnvOrDefault("REDIS_PASSWORD", "");
    
    // BigQuery configuration from environment
    private static final String BIGQUERY_PROJECT_ID = getEnvOrDefault("BIGQUERY_PROJECT_ID", "your-project-id");
    private static final String BIGQUERY_DATASET = getEnvOrDefault("BIGQUERY_DATASET", "analytics");
    private static final String BIGQUERY_TABLE = getEnvOrDefault("BIGQUERY_TABLE", "engagement_events");
    
    // Side outputs for error handling
    private static final OutputTag<String> PARSING_ERRORS = new OutputTag<String>("parsing-errors") {};
    private static final OutputTag<EngagementEvent> ENRICHMENT_ERRORS = new OutputTag<EngagementEvent>("enrichment-errors") {};
    
    public static void main(String[] args) throws Exception {
        // Set up the streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // Configure for high throughput and exactly-once processing
        configureEnvironment(env);
        
        // Create Kafka sources
        KafkaSource<String> engagementEventsSource = createEngagementEventsSource();
        KafkaSource<String> contentSource = createContentSource();
        
        // Create engagement events stream
        SingleOutputStreamOperator<EngagementEvent> engagementStream = env
            .fromSource(engagementEventsSource, WatermarkStrategy.noWatermarks(), "engagement-events-source")
            .process(new EngagementEventParser())
            .name("parse-engagement-events");
        
        // Create content lookup stream (broadcast)
        DataStream<ContentInfo> contentStream = env
            .fromSource(contentSource, WatermarkStrategy.noWatermarks(), "content-source")
            .map(new ContentParser())
            .name("parse-content");
        
        // Enrich engagement events with content information
        SingleOutputStreamOperator<EnrichedEvent> enrichedStream = engagementStream
            .keyBy((KeySelector<EngagementEvent, String>) EngagementEvent::getContentId)
            .connect(contentStream.keyBy((KeySelector<ContentInfo, String>) ContentInfo::getId))
            .process(new EngagementEnricher())
            .name("enrich-events");
        
        // Fan out to multiple sinks
        
        // 1. Redis sink for real-time aggregations (<5 seconds)
        enrichedStream
            .addSink(new RedisSink(REDIS_HOST, REDIS_PORT, REDIS_PASSWORD))
            .name("redis-sink")
            .setParallelism(4); // Optimize for low latency
        
        // 2. BigQuery sink for analytics (30-minute partitioning)
        enrichedStream
            .addSink(new BigQuerySink(BIGQUERY_PROJECT_ID, BIGQUERY_DATASET, BIGQUERY_TABLE))
            .name("bigquery-sink")
            .setParallelism(2); // Optimize for throughput
        
        // 3. External system sink (placeholder - not implemented as it's not important now)
        // enrichedStream.addSink(new ExternalSystemSink()).name("external-sink");
        
        // Handle error streams
        handleErrorStreams(engagementStream, enrichedStream);
        
        // Execute the job
        LOG.info("Starting Engagement Streaming Job...");
        LOG.info("Configuration - Kafka: {}, Redis: {}:{}, BigQuery: {}.{}.{}", 
                KAFKA_BOOTSTRAP_SERVERS, REDIS_HOST, REDIS_PORT, 
                BIGQUERY_PROJECT_ID, BIGQUERY_DATASET, BIGQUERY_TABLE);
        env.execute("Engagement Streaming Job");
    }
    
    private static void configureEnvironment(StreamExecutionEnvironment env) {
        // Set parallelism from environment or default
        int parallelism = Integer.parseInt(getEnvOrDefault("FLINK_PARALLELISM", "16"));
        env.setParallelism(parallelism);
        
        // Enable checkpointing for exactly-once semantics
        int checkpointInterval = Integer.parseInt(getEnvOrDefault("FLINK_CHECKPOINT_INTERVAL", "30000"));
        env.enableCheckpointing(checkpointInterval);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5000);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);
        
        // Configure state backend
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("file:///tmp/flink-checkpoints");
        
        // Configure restart strategy
        env.setRestartStrategy(RestartStrategies
            .exponentialDelayRestart(
                Time.seconds(1),
                Time.minutes(10),
                1.1,
                Time.minutes(5),
                0.1
            ));
    }
    
    private static KafkaSource<String> createEngagementEventsSource() {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS);
        props.setProperty("group.id", CONSUMER_GROUP);
        props.setProperty("enable.auto.commit", "false");
        props.setProperty("auto.offset.reset", "latest");
        
        return KafkaSource.<String>builder()
            .setBootstrapServers(KAFKA_BOOTSTRAP_SERVERS)
            .setTopics(ENGAGEMENT_EVENTS_TOPIC)
            .setGroupId(CONSUMER_GROUP)
            .setStartingOffsets(OffsetsInitializer.latest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .setProperties(props)
            .build();
    }
    
    private static KafkaSource<String> createContentSource() {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS);
        props.setProperty("group.id", CONSUMER_GROUP + "-content");
        props.setProperty("enable.auto.commit", "false");
        props.setProperty("auto.offset.reset", "earliest"); // Load all content data
        
        return KafkaSource.<String>builder()
            .setBootstrapServers(KAFKA_BOOTSTRAP_SERVERS)
            .setTopics(CONTENT_TOPIC)
            .setGroupId(CONSUMER_GROUP + "-content")
            .setStartingOffsets(OffsetsInitializer.earliest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .setProperties(props)
            .build();
    }
    
    private static void handleErrorStreams(SingleOutputStreamOperator<EngagementEvent> engagementStream,
                                         SingleOutputStreamOperator<EnrichedEvent> enrichedStream) {
        // Handle parsing errors
        engagementStream.getSideOutput(PARSING_ERRORS)
            .map(error -> {
                LOG.error("Parsing error: {}", error);
                return error;
            })
            .name("log-parsing-errors");
        
        // Handle enrichment errors
        enrichedStream.getSideOutput(ENRICHMENT_ERRORS)
            .map(event -> {
                LOG.error("Enrichment error for event: {}", event);
                return event;
            })
            .name("log-enrichment-errors");
    }
    
    /**
     * Parses Debezium CDC messages for engagement events
     */
    public static class EngagementEventParser extends ProcessFunction<String, EngagementEvent> {
        private transient ObjectMapper objectMapper;
        
        @Override
        public void open(Configuration parameters) throws Exception {
            objectMapper = new ObjectMapper();
            objectMapper.registerModule(new JavaTimeModule());
        }
        
        @Override
        public void processElement(String value, Context ctx, Collector<EngagementEvent> out) throws Exception {
            try {
                // Parse Debezium CDC message
                JsonNode root = objectMapper.readTree(value);
                JsonNode payload = root.get("payload");
                
                if (payload == null || payload.get("after") == null) {
                    // Handle DELETE operations or schema changes
                    return;
                }
                
                JsonNode after = payload.get("after");
                
                // Extract fields
                EngagementEvent event = new EngagementEvent();
                event.setId(after.get("id").asLong());
                event.setContentId(after.get("content_id").asText());
                event.setUserId(after.get("user_id").asText());
                event.setEventType(after.get("event_type").asText());
                
                // Parse timestamp
                String eventTsStr = after.get("event_ts").asText();
                event.setEventTs(LocalDateTime.parse(eventTsStr, 
                    DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS")));
                
                // Handle nullable fields
                if (!after.get("duration_ms").isNull()) {
                    event.setDurationMs(after.get("duration_ms").asInt());
                }
                
                event.setDevice(after.get("device").asText());
                
                if (!after.get("raw_payload").isNull()) {
                    event.setRawPayload(after.get("raw_payload"));
                }
                
                out.collect(event);
                
            } catch (Exception e) {
                LOG.warn("Failed to parse engagement event: {}", value, e);
                ctx.output(PARSING_ERRORS, "Parse error: " + e.getMessage() + " | Raw: " + value);
            }
        }
    }
    
    /**
     * Parses content information from CDC stream
     */
    public static class ContentParser extends RichMapFunction<String, ContentInfo> {
        private transient ObjectMapper objectMapper;
        
        @Override
        public void open(Configuration parameters) throws Exception {
            objectMapper = new ObjectMapper();
            objectMapper.registerModule(new JavaTimeModule());
        }
        
        @Override
        public ContentInfo map(String value) throws Exception {
            try {
                JsonNode root = objectMapper.readTree(value);
                JsonNode payload = root.get("payload");
                
                if (payload == null || payload.get("after") == null) {
                    return null;
                }
                
                JsonNode after = payload.get("after");
                
                ContentInfo content = new ContentInfo();
                content.setId(after.get("id").asText());
                content.setSlug(after.get("slug").asText());
                content.setTitle(after.get("title").asText());
                content.setContentType(after.get("content_type").asText());
                content.setLengthSeconds(after.get("length_seconds").asInt());
                
                String publishTsStr = after.get("publish_ts").asText();
                content.setPublishTs(LocalDateTime.parse(publishTsStr, 
                    DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSS")));
                
                return content;
                
            } catch (Exception e) {
                LOG.warn("Failed to parse content: {}", value, e);
                return null;
            }
        }
    }
    
    /**
     * Enriches engagement events with content information
     */
    public static class EngagementEnricher extends org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction<String, EngagementEvent, ContentInfo, EnrichedEvent> {
        private transient MapState<String, ContentInfo> contentState;
        
        @Override
        public void open(Configuration parameters) throws Exception {
            // Initialize state for content lookup
            MapStateDescriptor<String, ContentInfo> contentStateDescriptor = 
                new MapStateDescriptor<>("content-state", String.class, ContentInfo.class);
            contentState = getRuntimeContext().getMapState(contentStateDescriptor);
        }
        
        @Override
        public void processElement1(EngagementEvent event, Context ctx, Collector<EnrichedEvent> out) throws Exception {
            try {
                // Lookup content information
                ContentInfo content = contentState.get(event.getContentId());
                
                if (content != null) {
                    // Create enriched event
                    EnrichedEvent enrichedEvent = new EnrichedEvent(event, content);
                    out.collect(enrichedEvent);
                } else {
                    // Content not found - send to error stream
                    LOG.debug("Content not found for event: {}", event.getContentId());
                    ctx.output(ENRICHMENT_ERRORS, event);
                }
                
            } catch (Exception e) {
                LOG.error("Enrichment failed for event: {}", event, e);
                ctx.output(ENRICHMENT_ERRORS, event);
            }
        }
        
        @Override
        public void processElement2(ContentInfo content, Context ctx, Collector<EnrichedEvent> out) throws Exception {
            // Update content state
            if (content != null) {
                contentState.put(content.getId(), content);
                LOG.debug("Updated content state for: {}", content.getId());
            }
        }
    }
}