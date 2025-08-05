package com.cdc.streaming.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;

import java.time.LocalDateTime;
import java.util.Objects;

/**
 * Raw engagement event from Kafka CDC stream
 */
public class EngagementEvent {
    @JsonProperty("id")
    private Long id;

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

    // Default constructor
    public EngagementEvent() {}

    // Constructor
    public EngagementEvent(Long id, String contentId, String userId, String eventType, 
                          LocalDateTime eventTs, Integer durationMs, String device, JsonNode rawPayload) {
        this.id = id;
        this.contentId = contentId;
        this.userId = userId;
        this.eventType = eventType;
        this.eventTs = eventTs;
        this.durationMs = durationMs;
        this.device = device;
        this.rawPayload = rawPayload;
    }

    // Getters and Setters
    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }

    public String getContentId() { return contentId; }
    public void setContentId(String contentId) { this.contentId = contentId; }

    public String getUserId() { return userId; }
    public void setUserId(String userId) { this.userId = userId; }

    public String getEventType() { return eventType; }
    public void setEventType(String eventType) { this.eventType = eventType; }

    public LocalDateTime getEventTs() { return eventTs; }
    public void setEventTs(LocalDateTime eventTs) { this.eventTs = eventTs; }

    public Integer getDurationMs() { return durationMs; }
    public void setDurationMs(Integer durationMs) { this.durationMs = durationMs; }

    public String getDevice() { return device; }
    public void setDevice(String device) { this.device = device; }

    public JsonNode getRawPayload() { return rawPayload; }
    public void setRawPayload(JsonNode rawPayload) { this.rawPayload = rawPayload; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EngagementEvent that = (EngagementEvent) o;
        return Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public String toString() {
        return "EngagementEvent{" +
                "id=" + id +
                ", contentId='" + contentId + '\'' +
                ", userId='" + userId + '\'' +
                ", eventType='" + eventType + '\'' +
                ", eventTs=" + eventTs +
                ", durationMs=" + durationMs +
                ", device='" + device + '\'' +
                ", rawPayload=" + rawPayload +
                '}';
    }
}