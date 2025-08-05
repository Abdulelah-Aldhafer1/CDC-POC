#!/usr/bin/env python3
"""
High-volume data generator for engagement events
Generates realistic user interaction patterns to simulate 1M records per 5 minutes
"""

import os
import time
import json
import random
import logging
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Any

import psycopg2
from psycopg2.extras import execute_batch
from faker import Faker

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class EngagementDataGenerator:
    def __init__(self):
        self.fake = Faker()
        self.db_config = {
            'host': os.getenv('POSTGRES_HOST', 'localhost'),
            'port': int(os.getenv('POSTGRES_PORT', 5432)),
            'database': os.getenv('POSTGRES_DB', 'engagement_db'),
            'user': os.getenv('POSTGRES_USER', 'postgres'),
            'password': os.getenv('POSTGRES_PASSWORD', 'postgres')
        }
        
        # Generation parameters
        self.generation_rate = int(os.getenv('GENERATION_RATE', 200000))  # records per minute
        self.batch_size = 1000  # Insert batch size
        self.num_threads = 8    # Parallel insertion threads
        
        # Cache content and user data
        self.content_data = []
        self.user_pool = []
        self.devices = ['ios', 'android', 'web-chrome', 'web-safari', 'web-firefox', 'desktop']
        self.event_types = ['play', 'pause', 'finish', 'click']
        
        # Event type probabilities (realistic distribution)
        self.event_probabilities = {
            'play': 0.45,    # Most common - users start content
            'pause': 0.25,   # Common - users pause content
            'click': 0.20,   # Navigation/interaction clicks
            'finish': 0.10   # Least common - users finish content
        }
        
        logger.info(f"Initialized generator with rate: {self.generation_rate} records/minute")

    def connect_db(self):
        """Create database connection with retry logic"""
        max_retries = 5
        for attempt in range(max_retries):
            try:
                conn = psycopg2.connect(**self.db_config)
                conn.autocommit = False
                return conn
            except psycopg2.OperationalError as e:
                logger.warning(f"DB connection attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
                else:
                    raise

    def load_content_data(self):
        """Load content data for realistic event generation"""
        conn = self.connect_db()
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT id, slug, title, content_type, length_seconds 
                    FROM content
                """)
                self.content_data = cursor.fetchall()
                logger.info(f"Loaded {len(self.content_data)} content items")
        finally:
            conn.close()

    def generate_user_pool(self, size=10000):
        """Generate a pool of user UUIDs for realistic user behavior"""
        self.user_pool = [self.fake.uuid4() for _ in range(size)]
        logger.info(f"Generated user pool of {size} users")

    def generate_realistic_duration(self, content_length_seconds: int, event_type: str) -> int:
        """Generate realistic duration based on content length and event type"""
        if event_type == 'click':
            return None  # Clicks don't have duration
        
        length_ms = content_length_seconds * 1000
        
        if event_type == 'play':
            # Play events: 5% to 95% of content length
            return int(length_ms * (0.05 + random.random() * 0.90))
        elif event_type == 'pause':
            # Pause events: 10% to 80% of content length
            return int(length_ms * (0.10 + random.random() * 0.70))
        elif event_type == 'finish':
            # Finish events: 85% to 100% of content length
            return int(length_ms * (0.85 + random.random() * 0.15))
        
        return None

    def generate_event_batch(self, batch_size: int) -> List[tuple]:
        """Generate a batch of realistic engagement events"""
        events = []
        
        for _ in range(batch_size):
            # Select random content and user
            content = random.choice(self.content_data)
            content_id, slug, title, content_type, length_seconds = content
            user_id = random.choice(self.user_pool)
            
            # Select event type based on probabilities
            event_type = random.choices(
                list(self.event_probabilities.keys()),
                weights=list(self.event_probabilities.values())
            )[0]
            
            # Generate realistic timestamp (within last 10 minutes)
            event_ts = datetime.now() - timedelta(seconds=random.randint(0, 600))
            
            # Generate duration based on content and event type
            duration_ms = self.generate_realistic_duration(length_seconds, event_type)
            
            # Select random device
            device = random.choice(self.devices)
            
            # Generate realistic raw payload
            raw_payload = {
                'source': 'mobile_app' if device in ['ios', 'android'] else 'web_app',
                'version': f"{random.randint(1,3)}.{random.randint(0,9)}.{random.randint(0,9)}",
                'session_id': self.fake.uuid4(),
                'ip_address': self.fake.ipv4(),
                'user_agent': self.fake.user_agent()
            }
            
            # Add event-specific payload data
            if event_type == 'play':
                raw_payload.update({
                    'quality': random.choice(['SD', 'HD', '4K']),
                    'autoplay': random.choice([True, False])
                })
            elif event_type == 'finish':
                raw_payload.update({
                    'completion_rate': random.uniform(0.85, 1.0),
                    'rating': random.randint(1, 5) if random.random() < 0.3 else None
                })
            elif event_type == 'click':
                raw_payload.update({
                    'element': random.choice(['play_button', 'share_button', 'like_button', 'subscribe']),
                    'coordinates': {'x': random.randint(0, 1920), 'y': random.randint(0, 1080)}
                })
            
            events.append((
                content_id,
                user_id,
                event_type,
                event_ts,
                duration_ms,
                device,
                json.dumps(raw_payload)
            ))
        
        return events

    def insert_batch(self, events: List[tuple], thread_id: int) -> int:
        """Insert a batch of events into the database"""
        conn = None
        try:
            conn = self.connect_db()
            with conn.cursor() as cursor:
                execute_batch(
                    cursor,
                    """
                    INSERT INTO engagement_events 
                    (content_id, user_id, event_type, event_ts, duration_ms, device, raw_payload)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """,
                    events,
                    page_size=self.batch_size
                )
                conn.commit()
                return len(events)
        except Exception as e:
            logger.error(f"Thread {thread_id} - Insert failed: {e}")
            if conn:
                conn.rollback()
            return 0
        finally:
            if conn:
                conn.close()

    def generate_continuous(self):
        """Generate data continuously at the specified rate"""
        logger.info("Starting continuous data generation...")
        
        # Load initial data
        self.load_content_data()
        self.generate_user_pool()
        
        if not self.content_data:
            logger.error("No content data found. Please ensure content table is populated.")
            return
        
        total_generated = 0
        start_time = time.time()
        
        while True:
            minute_start = time.time()
            
            # Calculate batches needed for this minute
            batches_per_minute = self.generation_rate // self.batch_size
            events_remainder = self.generation_rate % self.batch_size
            
            logger.info(f"Generating {self.generation_rate} events in {batches_per_minute} batches + {events_remainder} remainder")
            
            # Parallel batch generation and insertion
            with ThreadPoolExecutor(max_workers=self.num_threads) as executor:
                # Submit all batches for this minute
                futures = []
                
                # Full batches
                for i in range(batches_per_minute):
                    events = self.generate_event_batch(self.batch_size)
                    future = executor.submit(self.insert_batch, events, i)
                    futures.append(future)
                
                # Remainder batch
                if events_remainder > 0:
                    events = self.generate_event_batch(events_remainder)
                    future = executor.submit(self.insert_batch, events, batches_per_minute)
                    futures.append(future)
                
                # Wait for all insertions to complete
                minute_total = 0
                for future in as_completed(futures):
                    try:
                        inserted = future.result()
                        minute_total += inserted
                    except Exception as e:
                        logger.error(f"Batch insertion failed: {e}")
                
                total_generated += minute_total
                
            # Calculate timing
            minute_elapsed = time.time() - minute_start
            total_elapsed = time.time() - start_time
            
            logger.info(
                f"Minute complete: {minute_total} events inserted in {minute_elapsed:.2f}s | "
                f"Total: {total_generated} events in {total_elapsed:.2f}s | "
                f"Rate: {total_generated/(total_elapsed/60):.0f} events/min"
            )
            
            # Sleep for the remainder of the minute
            sleep_time = max(0, 60 - minute_elapsed)
            if sleep_time > 0:
                time.sleep(sleep_time)

    def generate_burst(self, num_events: int):
        """Generate a burst of events for testing"""
        logger.info(f"Generating burst of {num_events} events...")
        
        self.load_content_data()
        self.generate_user_pool()
        
        if not self.content_data:
            logger.error("No content data found.")
            return
        
        batches = num_events // self.batch_size
        remainder = num_events % self.batch_size
        
        total_inserted = 0
        start_time = time.time()
        
        with ThreadPoolExecutor(max_workers=self.num_threads) as executor:
            futures = []
            
            # Full batches
            for i in range(batches):
                events = self.generate_event_batch(self.batch_size)
                future = executor.submit(self.insert_batch, events, i)
                futures.append(future)
            
            # Remainder batch
            if remainder > 0:
                events = self.generate_event_batch(remainder)
                future = executor.submit(self.insert_batch, events, batches)
                futures.append(future)
            
            # Wait for completion
            for future in as_completed(futures):
                try:
                    inserted = future.result()
                    total_inserted += inserted
                except Exception as e:
                    logger.error(f"Batch failed: {e}")
        
        elapsed = time.time() - start_time
        rate = total_inserted / elapsed if elapsed > 0 else 0
        
        logger.info(f"Burst complete: {total_inserted} events in {elapsed:.2f}s ({rate:.0f} events/sec)")

def main():
    generator = EngagementDataGenerator()
    
    # Check if we should run in burst mode for quick testing
    if os.getenv('BURST_MODE', '').lower() == 'true':
        burst_count = int(os.getenv('BURST_COUNT', 10000))
        generator.generate_burst(burst_count)
    else:
        # Continuous generation mode
        generator.generate_continuous()

if __name__ == "__main__":
    main()