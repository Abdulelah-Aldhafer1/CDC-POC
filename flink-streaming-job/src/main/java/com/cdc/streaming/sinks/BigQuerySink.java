package com.cdc.streaming.sinks;

import com.cdc.streaming.model.EnrichedEvent;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.cloud.bigquery.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * BigQuery sink with 30-minute partitioning and batch insertion
 * Implements buffering and automatic flushing based on time and size
 */
public class BigQuerySink extends RichSinkFunction<EnrichedEvent> implements CheckpointedFunction {
    private static final Logger LOG = LoggerFactory.getLogger(BigQuerySink.class);
    
    private final String projectId;
    private final String datasetId;
    private final String tableId;
    private final int maxBatchSize;
    private final long flushIntervalMs;
    
    private transient BigQuery bigQuery;
    private transient TableId table;
    private transient ObjectMapper objectMapper;
    private transient BlockingQueue<EnrichedEvent> eventBuffer;
    private transient Thread flushThread;
    private transient volatile boolean running;
    
    // Buffer state
    private transient long lastFlushTime;
    
    public BigQuerySink(String projectId, String datasetId, String tableId) {
        this(projectId, datasetId, tableId, 10000, 30000); // 10k records or 30 seconds
    }
    
    public BigQuerySink(String projectId, String datasetId, String tableId, 
                       int maxBatchSize, long flushIntervalMs) {
        this.projectId = projectId;
        this.datasetId = datasetId;
        this.tableId = tableId;
        this.maxBatchSize = maxBatchSize;
        this.flushIntervalMs = flushIntervalMs;
    }
    
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        
        // Initialize BigQuery client
        bigQuery = BigQueryOptions.getDefaultInstance().getService();
        table = TableId.of(projectId, datasetId, tableId);
        
        // Initialize Jackson ObjectMapper
        objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
        
        // Initialize buffer
        eventBuffer = new LinkedBlockingQueue<>(maxBatchSize * 2); // Allow some overflow
        lastFlushTime = System.currentTimeMillis();
        running = true;
        
        
        // Start flush thread
        startFlushThread();
        
        // Ensure table exists and has correct schema
        ensureTableExists();
        
        LOG.info("BigQuery sink initialized for table: {}.{}.{}", projectId, datasetId, tableId);
    }
    
    private void ensureTableExists() {
        try {
            Table existingTable = bigQuery.getTable(table);
            if (existingTable == null) {
                LOG.info("Creating BigQuery table: {}.{}.{}", projectId, datasetId, tableId);
                createTable();
            } else {
                LOG.info("BigQuery table exists: {}.{}.{}", projectId, datasetId, tableId);
            }
        } catch (Exception e) {
            LOG.error("Failed to check/create BigQuery table", e);
            throw new RuntimeException("BigQuery table initialization failed", e);
        }
    }
    
    private void createTable() {
        // Define table schema with 30-minute partitioning
        Schema schema = Schema.of(
            Field.of("event_id", StandardSQLTypeName.INT64),
            Field.of("content_id", StandardSQLTypeName.STRING),
            Field.of("user_id", StandardSQLTypeName.STRING),
            Field.of("event_type", StandardSQLTypeName.STRING),
            Field.of("event_ts", StandardSQLTypeName.TIMESTAMP),
            Field.of("duration_ms", StandardSQLTypeName.INT64),
            Field.of("device", StandardSQLTypeName.STRING),
            Field.of("raw_payload", StandardSQLTypeName.JSON),
            Field.of("content_slug", StandardSQLTypeName.STRING),
            Field.of("content_title", StandardSQLTypeName.STRING),
            Field.of("content_type", StandardSQLTypeName.STRING),
            Field.of("length_seconds", StandardSQLTypeName.INT64),
            Field.of("publish_ts", StandardSQLTypeName.TIMESTAMP),
            Field.of("engagement_seconds", StandardSQLTypeName.INT64),
            Field.of("engagement_pct", StandardSQLTypeName.NUMERIC)
        );
        
        // Configure time partitioning (30-minute partitions)
        TimePartitioning timePartitioning = TimePartitioning.newBuilder(TimePartitioning.Type.HOUR)
            .setField("event_ts")
            .build();
        
        // Configure clustering for better query performance
        List<String> clusteringFields = new ArrayList<>();
        clusteringFields.add("content_type");
        clusteringFields.add("event_type");
        Clustering clustering = Clustering.newBuilder()
            .setFields(clusteringFields)
            .build();
        
        TableDefinition tableDefinition = StandardTableDefinition.newBuilder()
            .setSchema(schema)
            .setTimePartitioning(timePartitioning)
            .setClustering(clustering)
            .build();
        
        TableInfo tableInfo = TableInfo.newBuilder(table, tableDefinition).build();
        
        bigQuery.create(tableInfo);
        LOG.info("Created BigQuery table with 30-minute partitioning and clustering");
    }
    
    private void startFlushThread() {
        flushThread = new Thread(this::flushLoop, "BigQuery-Flush-Thread");
        flushThread.setDaemon(true);
        flushThread.start();
    }
    
    private void flushLoop() {
        while (running) {
            try {
                boolean shouldFlush = false;
                
                // Check if we should flush based on time
                long currentTime = System.currentTimeMillis();
                if (currentTime - lastFlushTime >= flushIntervalMs) {
                    shouldFlush = true;
                }
                
                // Check if we should flush based on buffer size
                if (eventBuffer.size() >= maxBatchSize) {
                    shouldFlush = true;
                }
                
                if (shouldFlush && !eventBuffer.isEmpty()) {
                    flushBuffer();
                }
                
                // Sleep for a short period before checking again
                Thread.sleep(1000);
                
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOG.info("Flush thread interrupted");
                break;
            } catch (Exception e) {
                LOG.error("Error in flush loop", e);
            }
        }
        
        // Final flush on shutdown
        if (!eventBuffer.isEmpty()) {
            try {
                flushBuffer();
            } catch (Exception e) {
                LOG.error("Error during final flush", e);
            }
        }
    }
    
    @Override
    public void invoke(EnrichedEvent event, Context context) throws Exception {
        // Add to buffer (blocking if buffer is full)
        boolean added = eventBuffer.offer(event, 5, TimeUnit.SECONDS);
        if (!added) {
            LOG.warn("Failed to add event to buffer within timeout - buffer may be full");
        }
    }
    
    private void flushBuffer() {
        List<EnrichedEvent> batch = new ArrayList<>();
        
        // Drain events from buffer
        eventBuffer.drainTo(batch, maxBatchSize);
        
        if (batch.isEmpty()) {
            return;
        }
        
        try {
            // Convert events to BigQuery rows
            List<InsertAllRequest.RowToInsert> rows = new ArrayList<>();
            
            for (EnrichedEvent event : batch) {
                Map<String, Object> rowContent = convertEventToRowContent(event);
                rows.add(InsertAllRequest.RowToInsert.of(rowContent));
            }
            
            // Insert into BigQuery
            InsertAllRequest insertRequest = InsertAllRequest.newBuilder(table)
                .setRows(rows)
                .setSkipInvalidRows(false)
                .setIgnoreUnknownValues(false)
                .build();
            
            InsertAllResponse response = bigQuery.insertAll(insertRequest);
            
            if (response.hasErrors()) {
                LOG.error("BigQuery insertion errors: {}", response.getInsertErrors());
                
                // Log specific errors for debugging
                for (Map.Entry<Long, List<BigQueryError>> entry : response.getInsertErrors().entrySet()) {
                    LOG.error("Row {} errors: {}", entry.getKey(), entry.getValue());
                }
            } else {
                LOG.debug("Successfully inserted batch of {} events", batch.size());
            }
            
            lastFlushTime = System.currentTimeMillis();
            
        } catch (Exception e) {
            LOG.error("Failed to insert batch of {} events", batch.size(), e);
            
            // Re-queue events for retry (simple strategy)
            for (EnrichedEvent event : batch) {
                eventBuffer.offer(event);
            }
        }
    }
    
    private Map<String, Object> convertEventToRowContent(EnrichedEvent event) {
        Map<String, Object> rowContent = new java.util.HashMap<>();
        rowContent.put("event_id", event.getEventId());
        rowContent.put("content_id", event.getContentId());
        rowContent.put("user_id", event.getUserId());
        rowContent.put("event_type", event.getEventType());
        rowContent.put("event_ts", event.getEventTs() != null ? 
            event.getEventTs().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS")) : null);
        rowContent.put("duration_ms", event.getDurationMs());
        rowContent.put("device", event.getDevice());
        rowContent.put("raw_payload", event.getRawPayload() != null ? event.getRawPayload().toString() : null);
        rowContent.put("content_slug", event.getContentSlug());
        rowContent.put("content_title", event.getContentTitle());
        rowContent.put("content_type", event.getContentType());
        rowContent.put("length_seconds", event.getLengthSeconds());
        rowContent.put("publish_ts", event.getPublishTs() != null ? 
            event.getPublishTs().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS")) : null);
        rowContent.put("engagement_seconds", event.getEngagementSeconds());
        rowContent.put("engagement_pct", event.getEngagementPct() != null ? 
            event.getEngagementPct().doubleValue() : null);
        
        return rowContent;
    }
    
    @Override
    public void close() throws Exception {
        running = false;
        
        if (flushThread != null) {
            flushThread.interrupt();
            flushThread.join(5000); // Wait up to 5 seconds
        }
        
        super.close();
        LOG.info("BigQuery sink closed");
    }
    
    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        // Flush any pending events before checkpoint
        if (!eventBuffer.isEmpty()) {
            flushBuffer();
        }
    }
    
    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        // Nothing to restore
    }
}