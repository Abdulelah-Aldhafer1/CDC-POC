package com.cdc.streaming.sinks;

import com.cdc.streaming.model.EnrichedEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisException;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.TimeUnit;

/**
 * Redis sink for real-time engagement metrics
 * Implements sliding window aggregations for top content by engagement
 */
public class RedisSink extends RichSinkFunction<EnrichedEvent> implements CheckpointedFunction {
    private static final Logger LOG = LoggerFactory.getLogger(RedisSink.class);
    
    private final String redisHost;
    private final int redisPort;
    private final String redisPassword;
    private final int connectionTimeout;
    private final int socketTimeout;
    private final int maxRetries;
    
    private transient JedisPool jedisPool;
    private transient ObjectMapper objectMapper;
    
    // Redis key prefixes
    private static final String TOP_ENGAGEMENT_KEY = "top_engagement:10min";
    private static final String CONTENT_STATS_PREFIX = "content_stats:";
    private static final String USER_ENGAGEMENT_PREFIX = "user_engagement:";
    private static final String ENGAGEMENT_WINDOW_PREFIX = "engagement_window:";
    
    // TTL settings (in seconds)
    private static final int TOP_ENGAGEMENT_TTL = 600; // 10 minutes
    private static final int CONTENT_STATS_TTL = 3600; // 1 hour
    private static final int USER_ENGAGEMENT_TTL = 1800; // 30 minutes
    private static final int ENGAGEMENT_WINDOW_TTL = 600; // 10 minutes
    
    public RedisSink(String redisHost, int redisPort, String redisPassword) {
        this.redisHost = redisHost;
        this.redisPort = redisPort;
        this.redisPassword = redisPassword;
        this.connectionTimeout = 5000; // 5 seconds
        this.socketTimeout = 2000; // 2 seconds
        this.maxRetries = 3;
    }
    
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        
        // Initialize Jackson ObjectMapper
        objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
        
        
        // Initialize Redis connection pool
        initializeRedisPool();
        
        LOG.info("Redis sink initialized with host: {}:{}", redisHost, redisPort);
    }
    
    private void initializeRedisPool() {
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(20);
        poolConfig.setMaxIdle(10);
        poolConfig.setMinIdle(2);
        poolConfig.setTestOnBorrow(true);
        poolConfig.setTestOnReturn(true);
        poolConfig.setTestWhileIdle(true);
        poolConfig.setMaxWaitMillis(3000);
        
        if (redisPassword != null && !redisPassword.isEmpty()) {
            jedisPool = new JedisPool(poolConfig, redisHost, redisPort, 
                                    connectionTimeout, redisPassword);
        } else {
            jedisPool = new JedisPool(poolConfig, redisHost, redisPort);
        }
    }
    
    @Override
    public void invoke(EnrichedEvent event, Context context) throws Exception {
        int retryCount = 0;
        Exception lastException = null;
        
        while (retryCount < maxRetries) {
            try (Jedis jedis = jedisPool.getResource()) {
                // Process the event
                processEvent(jedis, event);
                
                LOG.debug("Successfully processed event {}", event.getEventId());
                return;
                
            } catch (JedisException e) {
                lastException = e;
                retryCount++;
                
                LOG.warn("Redis operation failed (attempt {}/{}): {}", 
                        retryCount, maxRetries, e.getMessage());
                
                if (retryCount < maxRetries) {
                    try {
                        Thread.sleep(100 * retryCount); // Exponential backoff
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("Interrupted during retry", ie);
                    }
                }
            }
        }
        
        // All retries failed
        LOG.error("Failed to process event {} after {} retries", 
                 event.getEventId(), maxRetries, lastException);
        
        // For now, we'll log and continue. In production, you might want to:
        // 1. Send to dead letter queue
        // 2. Alert monitoring systems
        // 3. Implement circuit breaker pattern
    }
    
    private void processEvent(Jedis jedis, EnrichedEvent event) {
        // Only process events with valid engagement percentage
        if (!event.hasValidEngagement()) {
            LOG.debug("Skipping event {} - no valid engagement data", event.getEventId());
            return;
        }
        
        String contentId = event.getContentId();
        String userId = event.getUserId();
        BigDecimal engagementPct = event.getEngagementPct();
        LocalDateTime eventTs = event.getEventTs();
        
        // Use Redis pipeline for batch operations
        redis.clients.jedis.Pipeline pipeline = jedis.pipelined();
        
        try {
            // 1. Update top engagement leaderboard (global)
            updateTopEngagement(pipeline, contentId, engagementPct, event);
            
            // 2. Update content statistics
            updateContentStats(pipeline, contentId, engagementPct, event);
            
            // 3. Update user engagement (latest per user-content pair)
            updateUserEngagement(pipeline, userId, contentId, engagementPct, eventTs);
            
            // 4. Update time-based engagement window
            updateEngagementWindow(pipeline, contentId, engagementPct, eventTs);
            
            // Execute all operations
            pipeline.sync();
            
        } catch (Exception e) {
            // Note: pipeline.clear() method may not be available in this Jedis version
            // The pipeline will be automatically discarded when connection is closed
            LOG.warn("Pipeline execution failed, connection will be reset", e);
            throw new JedisException("Pipeline execution failed", e);
        }
    }
    
    private void updateTopEngagement(redis.clients.jedis.Pipeline pipeline, String contentId, 
                                   BigDecimal engagementPct, EnrichedEvent event) {
        // Add to sorted set for top engagement (score = engagement_pct)
        double score = engagementPct.doubleValue();
        
        // Use content metadata as member value for richer information
        String member = String.format("%s:%s:%s", 
            contentId, event.getContentType(), event.getContentTitle());
        
        pipeline.zadd(TOP_ENGAGEMENT_KEY, score, member);
        pipeline.expire(TOP_ENGAGEMENT_KEY, TOP_ENGAGEMENT_TTL);
        
        // Keep only top 100 items to prevent unbounded growth
        pipeline.zremrangeByRank(TOP_ENGAGEMENT_KEY, 0, -101);
    }
    
    private void updateContentStats(redis.clients.jedis.Pipeline pipeline, String contentId, 
                                  BigDecimal engagementPct, EnrichedEvent event) {
        String statsKey = CONTENT_STATS_PREFIX + contentId;
        
        // Store content statistics as hash
        pipeline.hset(statsKey, "latest_engagement", engagementPct.toString());
        pipeline.hset(statsKey, "content_type", event.getContentType());
        pipeline.hset(statsKey, "content_title", event.getContentTitle());
        pipeline.hset(statsKey, "last_updated", event.getEventTs().toString());
        pipeline.hincrBy(statsKey, "total_events", 1);
        
        // Set TTL
        pipeline.expire(statsKey, CONTENT_STATS_TTL);
        
        // Update running average (simplified - in production you'd use more sophisticated aggregation)
        pipeline.hset(statsKey, "event_type", event.getEventType());
        pipeline.hset(statsKey, "device", event.getDevice());
    }
    
    private void updateUserEngagement(redis.clients.jedis.Pipeline pipeline, String userId, String contentId, 
                                    BigDecimal engagementPct, LocalDateTime eventTs) {
        String userKey = USER_ENGAGEMENT_PREFIX + userId + ":" + contentId;
        
        // Store latest engagement for this user-content pair
        pipeline.hset(userKey, "engagement_pct", engagementPct.toString());
        pipeline.hset(userKey, "last_updated", eventTs.toString());
        
        // Set TTL
        pipeline.expire(userKey, USER_ENGAGEMENT_TTL);
    }
    
    private void updateEngagementWindow(redis.clients.jedis.Pipeline pipeline, String contentId, 
                                      BigDecimal engagementPct, LocalDateTime eventTs) {
        // Create minute-level buckets for sliding window calculations
        String minuteKey = ENGAGEMENT_WINDOW_PREFIX + 
                          eventTs.format(DateTimeFormatter.ofPattern("yyyy-MM-dd:HH:mm"));
        
        // Store engagement events in time buckets
        String member = contentId + ":" + engagementPct.toString();
        pipeline.zadd(minuteKey, engagementPct.doubleValue(), member);
        pipeline.expire(minuteKey, ENGAGEMENT_WINDOW_TTL);
    }
    
    @Override
    public void close() throws Exception {
        if (jedisPool != null && !jedisPool.isClosed()) {
            jedisPool.close();
        }
        super.close();
        LOG.info("Redis sink closed");
    }
    
    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        // Nothing to snapshot for this sink
    }
    
    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        // Nothing to initialize
    }
}