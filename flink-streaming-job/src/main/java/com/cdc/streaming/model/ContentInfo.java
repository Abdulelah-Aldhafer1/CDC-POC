package com.cdc.streaming.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.LocalDateTime;
import java.util.Objects;

/**
 * Content information for enrichment
 */
public class ContentInfo {
    @JsonProperty("id")
    private String id;

    @JsonProperty("slug")
    private String slug;

    @JsonProperty("title")
    private String title;

    @JsonProperty("content_type")
    private String contentType;

    @JsonProperty("length_seconds")
    private Integer lengthSeconds;

    @JsonProperty("publish_ts")
    @JsonFormat(pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSSSS")
    private LocalDateTime publishTs;

    // Default constructor
    public ContentInfo() {}

    // Constructor
    public ContentInfo(String id, String slug, String title, String contentType, 
                      Integer lengthSeconds, LocalDateTime publishTs) {
        this.id = id;
        this.slug = slug;
        this.title = title;
        this.contentType = contentType;
        this.lengthSeconds = lengthSeconds;
        this.publishTs = publishTs;
    }

    // Getters and Setters
    public String getId() { return id; }
    public void setId(String id) { this.id = id; }

    public String getSlug() { return slug; }
    public void setSlug(String slug) { this.slug = slug; }

    public String getTitle() { return title; }
    public void setTitle(String title) { this.title = title; }

    public String getContentType() { return contentType; }
    public void setContentType(String contentType) { this.contentType = contentType; }

    public Integer getLengthSeconds() { return lengthSeconds; }
    public void setLengthSeconds(Integer lengthSeconds) { this.lengthSeconds = lengthSeconds; }

    public LocalDateTime getPublishTs() { return publishTs; }
    public void setPublishTs(LocalDateTime publishTs) { this.publishTs = publishTs; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ContentInfo that = (ContentInfo) o;
        return Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public String toString() {
        return "ContentInfo{" +
                "id='" + id + '\'' +
                ", slug='" + slug + '\'' +
                ", title='" + title + '\'' +
                ", contentType='" + contentType + '\'' +
                ", lengthSeconds=" + lengthSeconds +
                ", publishTs=" + publishTs +
                '}';
    }
}