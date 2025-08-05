package com.cdc.streaming.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDateTime;
import java.util.Objects;

/**
 * Enriched engagement event with content information and calculated fields
 */
public class EnrichedEvent {
    // Original event fields
    @JsonProperty("event_id")
    private Long eventId;

    @JsonProperty("content_id")
    private String contentId;

    @JsonProperty("user_id")
    private String userId;

    @JsonProperty("event_type")
    private String eventType;

    @JsonProperty("event_ts")
    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSSSS")
    private LocalDateTime eventTs;

    @JsonProperty("duration_ms")
    private Integer durationMs;

    @JsonProperty("device")
    private String device;

    @JsonProperty("raw_payload")
    private JsonNode rawPayload;

    // Enriched fields from content table
    @JsonProperty("content_slug")
    private String contentSlug;

    @JsonProperty("content_title")
    private String contentTitle;

    @JsonProperty("content_type")
    private String contentType;

    @JsonProperty("length_seconds")
    private Integer lengthSeconds;

    @JsonProperty("publish_ts")
    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSSSS")
    private LocalDateTime publishTs;

    // Calculated fields
    @JsonProperty("engagement_seconds")
    private Integer engagementSeconds;

    @JsonProperty("engagement_pct")
    private BigDecimal engagementPct;

    // Default constructor
    public EnrichedEvent() {}

    // Constructor from EngagementEvent and ContentInfo
    public EnrichedEvent(EngagementEvent event, ContentInfo content) {
        this.eventId = event.getId();
        this.contentId = event.getContentId();
        this.userId = event.getUserId();
        this.eventType = event.getEventType();
        this.eventTs = event.getEventTs();
        this.durationMs = event.getDurationMs();
        this.device = event.getDevice();
        this.rawPayload = event.getRawPayload();

        if (content != null) {
            this.contentSlug = content.getSlug();
            this.contentTitle = content.getTitle();
            this.contentType = content.getContentType();
            this.lengthSeconds = content.getLengthSeconds();
            this.publishTs = content.getPublishTs();
        }

        // Calculate derived fields
        calculateDerivedFields();
    }

    private void calculateDerivedFields() {
        // Calculate engagement_seconds (convert ms to seconds)
        if (durationMs != null) {
            this.engagementSeconds = durationMs / 1000;
        }

        // Calculate engagement_pct
        if (engagementSeconds != null && lengthSeconds != null && lengthSeconds > 0) {
            BigDecimal engagementSec = new BigDecimal(engagementSeconds);
            BigDecimal lengthSec = new BigDecimal(lengthSeconds);
            this.engagementPct = engagementSec
                .divide(lengthSec, 4, RoundingMode.HALF_UP)
                .multiply(new BigDecimal("100"))
                .setScale(2, RoundingMode.HALF_UP);
        }
    }

    // Getters and Setters
    public Long getEventId() { return eventId; }
    public void setEventId(Long eventId) { this.eventId = eventId; }

    public String getContentId() { return contentId; }
    public void setContentId(String contentId) { this.contentId = contentId; }

    public String getUserId() { return userId; }
    public void setUserId(String userId) { this.userId = userId; }

    public String getEventType() { return eventType; }
    public void setEventType(String eventType) { this.eventType = eventType; }

    public LocalDateTime getEventTs() { return eventTs; }
    public void setEventTs(LocalDateTime eventTs) { this.eventTs = eventTs; }

    public Integer getDurationMs() { return durationMs; }
    public void setDurationMs(Integer durationMs) { 
        this.durationMs = durationMs;
        calculateDerivedFields(); // Recalculate when duration changes
    }

    public String getDevice() { return device; }
    public void setDevice(String device) { this.device = device; }

    public JsonNode getRawPayload() { return rawPayload; }
    public void setRawPayload(JsonNode rawPayload) { this.rawPayload = rawPayload; }

    public String getContentSlug() { return contentSlug; }
    public void setContentSlug(String contentSlug) { this.contentSlug = contentSlug; }

    public String getContentTitle() { return contentTitle; }
    public void setContentTitle(String contentTitle) { this.contentTitle = contentTitle; }

    public String getContentType() { return contentType; }
    public void setContentType(String contentType) { this.contentType = contentType; }

    public Integer getLengthSeconds() { return lengthSeconds; }
    public void setLengthSeconds(Integer lengthSeconds) { 
        this.lengthSeconds = lengthSeconds;
        calculateDerivedFields(); // Recalculate when length changes
    }

    public LocalDateTime getPublishTs() { return publishTs; }
    public void setPublishTs(LocalDateTime publishTs) { this.publishTs = publishTs; }

    public Integer getEngagementSeconds() { return engagementSeconds; }
    public void setEngagementSeconds(Integer engagementSeconds) { this.engagementSeconds = engagementSeconds; }

    public BigDecimal getEngagementPct() { return engagementPct; }
    public void setEngagementPct(BigDecimal engagementPct) { this.engagementPct = engagementPct; }

    // Utility methods
    public boolean hasValidEngagement() {
        return engagementPct != null && engagementPct.compareTo(BigDecimal.ZERO) >= 0;
    }

    public boolean isHighEngagement() {
        return engagementPct != null && engagementPct.compareTo(new BigDecimal("50")) >= 0;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EnrichedEvent that = (EnrichedEvent) o;
        return Objects.equals(eventId, that.eventId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventId);
    }

    @Override
    public String toString() {
        return "EnrichedEvent{" +
                "eventId=" + eventId +
                ", contentId='" + contentId + '\'' +
                ", userId='" + userId + '\'' +
                ", eventType='" + eventType + '\'' +
                ", contentType='" + contentType + '\'' +
                ", engagementPct=" + engagementPct +
                ", eventTs=" + eventTs +
                '}';
    }
}