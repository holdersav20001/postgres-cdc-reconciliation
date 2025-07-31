#!/usr/bin/env python3

import csv
import json
import random
import psycopg2
from datetime import datetime
from pathlib import Path
import argparse
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class OrderGenerator:
    def __init__(self, config_file='config.json'):
        self.config = self._load_config(config_file)
        self.feed_dir = Path(self.config['feed_dir'])
        self.feed_dir.mkdir(parents=True, exist_ok=True)
        
    def _load_config(self, config_file):
        try:
            with open(config_file) as f:
                return json.load(f)
        except FileNotFoundError:
            # Default configuration
            return {
                'feed_dir': 'feed_files',
                'db': {
                    'host': 'localhost',
                    'port': 5432,
                    'database': 'sourcedb',
                    'user': 'sourceuser',
                    'password': 'sourcepass'
                },
                'batch_size': 100,
                'customer_id_range': (1, 1000),
                'amount_range': (10.00, 1000.00)
            }

    def generate_order(self):
        """Generate a single random order"""
        return {
            'customer_id': random.randint(*self.config['customer_id_range']),
            'amount': round(random.uniform(*self.config['amount_range']), 2),
            'timestamp': datetime.now().isoformat()
        }

    def generate_batch(self, batch_size):
        """Generate a batch of orders"""
        return [self.generate_order() for _ in range(batch_size)]

    def create_feed_file(self, orders, batch_id):
        """Create a CSV feed file for the batch"""
        filename = self.feed_dir / f'orders_batch_{batch_id}.csv'
        with open(filename, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=['customer_id', 'amount', 'timestamp'])
            writer.writeheader()
            writer.writerows(orders)
        return filename

    def get_current_lsn(self, conn):
        """Get current PostgreSQL LSN"""
        with conn.cursor() as cur:
            cur.execute("SELECT pg_current_wal_lsn()::text")
            return cur.fetchone()[0]

    def create_batch_record(self, conn, batch_id, row_count):
        """Create a batch control record"""
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO batch_control 
                (schema_name, table_name, batch_id, status, row_count)
                VALUES ('public', 'orders', %s, 'IN_PROGRESS', %s)
                RETURNING id
            """, (batch_id, row_count))
            return cur.fetchone()[0]

    def update_batch_completion(self, conn, batch_id, lsn):
        """Update batch control record with completion status and LSN"""
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE batch_control 
                SET status = 'COMPLETED',
                    completion_timestamp = CURRENT_TIMESTAMP,
                    completion_lsn = %s
                WHERE schema_name = 'public' 
                AND table_name = 'orders'
                AND batch_id = %s
            """, (lsn, batch_id))

    def load_feed_file(self, conn, filename, batch_id):
        """Load a feed file into the database"""
        try:
            with conn.cursor() as cur:
                # Get starting LSN
                start_lsn = self.get_current_lsn(conn)
                logger.info(f"Starting LSN: {start_lsn}")

                # Create batch record
                with open(filename) as f:
                    row_count = sum(1 for _ in f) - 1  # Subtract header row
                self.create_batch_record(conn, batch_id, row_count)

                # Load data using COPY
                with open(filename) as f:
                    cur.copy_expert(
                        "COPY orders (customer_id, amount, timestamp) FROM STDIN WITH CSV HEADER",
                        f
                    )

                # Get ending LSN and update batch record
                end_lsn = self.get_current_lsn(conn)
                logger.info(f"Ending LSN: {end_lsn}")
                self.update_batch_completion(conn, batch_id, end_lsn)

                conn.commit()
                logger.info(f"Successfully loaded batch {batch_id} with {row_count} records")
                return True

        except Exception as e:
            conn.rollback()
            logger.error(f"Error loading batch {batch_id}: {str(e)}")
            return False

def main():
    parser = argparse.ArgumentParser(description='Generate and load order data')
    parser.add_argument('--batch-size', type=int, help='Number of orders to generate')
    parser.add_argument('--batch-id', type=int, help='Batch ID for this run')
    parser.add_argument('--config', help='Path to config file', default='config.json')
    args = parser.parse_args()

    generator = OrderGenerator(args.config)
    batch_size = args.batch_size or generator.config['batch_size']
    batch_id = args.batch_id or int(datetime.now().timestamp())

    logger.info(f"Generating batch {batch_id} with {batch_size} orders")
    
    # Generate and save orders to feed file
    orders = generator.generate_batch(batch_size)
    feed_file = generator.create_feed_file(orders, batch_id)
    logger.info(f"Created feed file: {feed_file}")

    # Connect to database and load the feed file
    try:
        with psycopg2.connect(**generator.config['db']) as conn:
            success = generator.load_feed_file(conn, feed_file, batch_id)
            if success:
                logger.info("Batch processing completed successfully")
            else:
                logger.error("Batch processing failed")
    except Exception as e:
        logger.error(f"Database connection error: {str(e)}")

if __name__ == '__main__':
    main()