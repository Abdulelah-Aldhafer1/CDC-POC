-- Initialize PostgreSQL database with tables and sample data

-- Enable logical replication
ALTER SYSTEM SET wal_level = logical;
ALTER SYSTEM SET max_replication_slots = 4;
ALTER SYSTEM SET max_wal_senders = 4;

-- Create content catalogue table
CREATE TABLE content (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    slug            TEXT UNIQUE NOT NULL,
    title           TEXT        NOT NULL,
    content_type    TEXT CHECK (content_type IN ('podcast', 'newsletter', 'video')),
    length_seconds  INTEGER, 
    publish_ts      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Create raw engagement telemetry table
CREATE TABLE engagement_events (
    id           BIGSERIAL PRIMARY KEY,
    content_id   UUID REFERENCES content(id),
    user_id      UUID,
    event_type   TEXT CHECK (event_type IN ('play', 'pause', 'finish', 'click')),
    event_ts     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    duration_ms  INTEGER,      -- nullable for events without duration
    device       TEXT,         -- e.g. "ios", "web‑safari"
    raw_payload  JSONB         -- anything extra the client sends
);

-- Create indexes for better performance
CREATE INDEX idx_engagement_events_content_id ON engagement_events(content_id);
CREATE INDEX idx_engagement_events_user_id ON engagement_events(user_id);
CREATE INDEX idx_engagement_events_event_ts ON engagement_events(event_ts);
CREATE INDEX idx_engagement_events_event_type ON engagement_events(event_type);

-- Create partition on engagement_events by date (for scaling)
CREATE TABLE engagement_events_template (LIKE engagement_events INCLUDING ALL);

-- Insert sample content data
INSERT INTO content (id, slug, title, content_type, length_seconds, publish_ts) VALUES
-- Podcasts
('550e8400-e29b-41d4-a716-446655440001', 'tech-talk-ep1', 'Tech Talk Episode 1: AI Revolution', 'podcast', 3600, '2025-01-01 08:00:00+00'),
('550e8400-e29b-41d4-a716-446655440002', 'business-insights-ep5', 'Business Insights: Market Trends 2025', 'podcast', 2400, '2025-01-02 09:00:00+00'),
('550e8400-e29b-41d4-a716-446655440003', 'health-wellness-ep12', 'Health & Wellness: Mental Health Tips', 'podcast', 1800, '2025-01-03 10:00:00+00'),
('550e8400-e29b-41d4-a716-446655440004', 'startup-stories-ep8', 'Startup Stories: From Zero to IPO', 'podcast', 4200, '2025-01-04 11:00:00+00'),
('550e8400-e29b-41d4-a716-446655440005', 'tech-news-daily-jan5', 'Tech News Daily: January 5th Edition', 'podcast', 900, '2025-01-05 07:00:00+00'),

-- Videos
('550e8400-e29b-41d4-a716-446655440006', 'coding-tutorial-react', 'React Hooks Tutorial for Beginners', 'video', 1200, '2025-01-01 14:00:00+00'),
('550e8400-e29b-41d4-a716-446655440007', 'product-demo-saas', 'SaaS Product Demo: Complete Walkthrough', 'video', 600, '2025-01-02 15:00:00+00'),
('550e8400-e29b-41d4-a716-446655440008', 'webinar-data-science', 'Data Science Webinar: ML in Production', 'video', 5400, '2025-01-03 16:00:00+00'),
('550e8400-e29b-41d4-a716-446655440009', 'conference-keynote-2025', '2025 Tech Conference Keynote', 'video', 2700, '2025-01-04 17:00:00+00'),
('550e8400-e29b-41d4-a716-446655440010', 'short-tip-productivity', 'Quick Productivity Tip #47', 'video', 180, '2025-01-05 12:00:00+00'),

-- Newsletters
('550e8400-e29b-41d4-a716-446655440011', 'weekly-digest-jan-w1', 'Weekly Tech Digest - Week 1', 'newsletter', 300, '2025-01-01 06:00:00+00'),
('550e8400-e29b-41d4-a716-446655440012', 'market-analysis-q4', 'Q4 Market Analysis Summary', 'newsletter', 480, '2025-01-02 06:00:00+00'),
('550e8400-e29b-41d4-a716-446655440013', 'developer-newsletter-15', 'Developer Newsletter Issue #15', 'newsletter', 240, '2025-01-03 06:00:00+00'),
('550e8400-e29b-41d4-a716-446655440014', 'fintech-updates-jan', 'FinTech Updates: January Edition', 'newsletter', 360, '2025-01-04 06:00:00+00'),
('550e8400-e29b-41d4-a716-446655440015', 'ai-research-digest', 'AI Research Weekly Digest', 'newsletter', 420, '2025-01-05 06:00:00+00');

-- Insert sample engagement events (initial batch)
INSERT INTO engagement_events (content_id, user_id, event_type, event_ts, duration_ms, device, raw_payload) VALUES
-- Some sample events for testing
('550e8400-e29b-41d4-a716-446655440001', gen_random_uuid(), 'play', NOW() - INTERVAL '10 minutes', 180000, 'ios', '{"source": "mobile_app", "version": "1.2.3"}'),
('550e8400-e29b-41d4-a716-446655440001', gen_random_uuid(), 'pause', NOW() - INTERVAL '8 minutes', 180000, 'web-chrome', '{"source": "web_app", "referrer": "google.com"}'),
('550e8400-e29b-41d4-a716-446655440002', gen_random_uuid(), 'play', NOW() - INTERVAL '5 minutes', 120000, 'android', '{"source": "mobile_app", "playlist": "business"}'),
('550e8400-e29b-41d4-a716-446655440003', gen_random_uuid(), 'finish', NOW() - INTERVAL '3 minutes', 1800000, 'web-safari', '{"source": "web_app", "completed": true}'),
('550e8400-e29b-41d4-a716-446655440006', gen_random_uuid(), 'click', NOW() - INTERVAL '2 minutes', NULL, 'web-chrome', '{"source": "web_app", "element": "play_button"}'),
('550e8400-e29b-41d4-a716-446655440007', gen_random_uuid(), 'play', NOW() - INTERVAL '1 minute', 300000, 'ios', '{"source": "mobile_app", "quality": "HD"}');

-- Create replication slot for Debezium
SELECT pg_create_logical_replication_slot('debezium_slot', 'pgoutput');

-- Create publication for all tables (required for Debezium)
CREATE PUBLICATION dbz_publication FOR ALL TABLES;

-- Grant necessary permissions for replication user
-- This will be done by the data generator service or manually

-- Add some utility functions for data generation
CREATE OR REPLACE FUNCTION generate_user_id() RETURNS UUID AS $$
BEGIN
    -- Generate consistent user IDs for testing
    RETURN ('00000000-0000-0000-0000-' || LPAD((RANDOM() * 999999)::INT::TEXT, 12, '0'))::UUID;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION generate_device() RETURNS TEXT AS $$
DECLARE
    devices TEXT[] := ARRAY['ios', 'android', 'web-chrome', 'web-safari', 'web-firefox', 'desktop'];
BEGIN
    RETURN devices[1 + (RANDOM() * array_length(devices, 1))::INT];
END;
$$ LANGUAGE plpgsql;

-- Create a function to generate realistic engagement duration
CREATE OR REPLACE FUNCTION generate_duration_ms(content_length_seconds INTEGER, event_type TEXT) RETURNS INTEGER AS $$
BEGIN
    CASE event_type
        WHEN 'click' THEN RETURN NULL;  -- Clicks don't have duration
        WHEN 'play' THEN 
            -- Random duration between 5% and 95% of content length
            RETURN (content_length_seconds * 1000 * (0.05 + RANDOM() * 0.90))::INTEGER;
        WHEN 'pause' THEN 
            -- Random duration between 10% and 80% of content length
            RETURN (content_length_seconds * 1000 * (0.10 + RANDOM() * 0.70))::INTEGER;
        WHEN 'finish' THEN 
            -- Finish events typically have 85-100% of content length
            RETURN (content_length_seconds * 1000 * (0.85 + RANDOM() * 0.15))::INTEGER;
        ELSE RETURN NULL;
    END CASE;
END;
$$ LANGUAGE plpgsql;