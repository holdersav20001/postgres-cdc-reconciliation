#!/usr/bin/env python3

import psycopg2
import json
import time
import logging
from datetime import datetime
import argparse

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class ReplicationVerifier:
    def __init__(self, config_file='config.json'):
        self.config = self._load_config(config_file)
        self.source_conn = None
        self.target_conn = None

    def _load_config(self, config_file):
        with open(config_file) as f:
            config = json.load(f)
            # Add target database configuration if not present
            if 'target_db' not in config:
                config['target_db'] = {
                    'host': 'localhost',
                    'port': 5433,
                    'database': 'targetdb',
                    'user': 'targetuser',
                    'password': 'targetpass'
                }
            return config

    def connect(self):
        """Establish connections to source and target databases"""
        try:
            self.source_conn = psycopg2.connect(**self.config['db'])
            self.target_conn = psycopg2.connect(**self.config['target_db'])
            logger.info("Successfully connected to source and target databases")
        except Exception as e:
            logger.error(f"Failed to connect to databases: {str(e)}")
            raise

    def close(self):
        """Close database connections"""
        if self.source_conn:
            self.source_conn.close()
        if self.target_conn:
            self.target_conn.close()

    def check_table_counts(self):
        """Compare record counts between source and target tables"""
        try:
            with self.source_conn.cursor() as src_cur, self.target_conn.cursor() as tgt_cur:
                src_cur.execute("SELECT COUNT(*) FROM orders")
                tgt_cur.execute("SELECT COUNT(*) FROM orders")
                
                src_count = src_cur.fetchone()[0]
                tgt_count = tgt_cur.fetchone()[0]
                
                logger.info(f"Source orders count: {src_count}")
                logger.info(f"Target orders count: {tgt_count}")
                
                return src_count == tgt_count, src_count, tgt_count
        except Exception as e:
            logger.error(f"Error checking table counts: {str(e)}")
            return False, 0, 0

    def check_batch_status(self, batch_id):
        """Check replication status for a specific batch"""
        try:
            with self.source_conn.cursor() as cur:
                cur.execute("""
                    SELECT * FROM check_latest_batch_replication_status('public', 'orders')
                    WHERE latest_batch_id = %s
                """, (batch_id,))
                result = cur.fetchone()
                
                if result:
                    status = {
                        'batch_id': result[2],
                        'slot_name': result[3],
                        'publication_name': result[4],
                        'replication_complete': result[7],
                        'minutes_since_completion': result[8]
                    }
                    logger.info(f"Batch {batch_id} status: {json.dumps(status, indent=2)}")
                    return status
                else:
                    logger.warning(f"No status found for batch {batch_id}")
                    return None
        except Exception as e:
            logger.error(f"Error checking batch status: {str(e)}")
            return None

    def verify_data_consistency(self, batch_id):
        """Verify data consistency for a specific batch"""
        try:
            with self.source_conn.cursor() as src_cur, self.target_conn.cursor() as tgt_cur:
                # Get source records for batch
                src_cur.execute("""
                    SELECT order_id, customer_id, amount, timestamp 
                    FROM orders 
                    WHERE order_id IN (
                        SELECT generate_series(
                            (SELECT MIN(order_id) FROM orders),
                            (SELECT MAX(order_id) FROM orders)
                        )
                    )
                    ORDER BY order_id
                """)
                source_records = src_cur.fetchall()

                # Get target records
                tgt_cur.execute("""
                    SELECT order_id, customer_id, amount, timestamp 
                    FROM orders 
                    ORDER BY order_id
                """)
                target_records = tgt_cur.fetchall()

                # Compare records
                mismatches = []
                for src_rec, tgt_rec in zip(source_records, target_records):
                    if src_rec != tgt_rec:
                        mismatches.append({
                            'order_id': src_rec[0],
                            'source': src_rec,
                            'target': tgt_rec
                        })

                if mismatches:
                    logger.warning(f"Found {len(mismatches)} mismatched records")
                    for mm in mismatches[:5]:  # Show first 5 mismatches
                        logger.warning(f"Mismatch for order_id {mm['order_id']}")
                    return False
                else:
                    logger.info("All records match between source and target")
                    return True

        except Exception as e:
            logger.error(f"Error verifying data consistency: {str(e)}")
            return False

    def monitor_replication(self, batch_id, timeout=300, interval=10):
        """Monitor replication progress with timeout"""
        start_time = time.time()
        while time.time() - start_time < timeout:
            status = self.check_batch_status(batch_id)
            if status and status['replication_complete']:
                logger.info(f"Replication completed for batch {batch_id}")
                return True
            
            counts_match, src_count, tgt_count = self.check_table_counts()
            if counts_match:
                logger.info("Record counts match between source and target")
                if self.verify_data_consistency(batch_id):
                    return True
            
            logger.info(f"Waiting for replication... Source: {src_count}, Target: {tgt_count}")
            time.sleep(interval)
        
        logger.error(f"Replication monitoring timed out after {timeout} seconds")
        return False

def main():
    parser = argparse.ArgumentParser(description='Verify CDC replication')
    parser.add_argument('--batch-id', type=int, required=True, help='Batch ID to verify')
    parser.add_argument('--config', default='config.json', help='Path to config file')
    parser.add_argument('--timeout', type=int, default=300, help='Monitoring timeout in seconds')
    parser.add_argument('--interval', type=int, default=10, help='Check interval in seconds')
    args = parser.parse_args()

    verifier = ReplicationVerifier(args.config)
    try:
        verifier.connect()
        success = verifier.monitor_replication(args.batch_id, args.timeout, args.interval)
        if success:
            logger.info("Verification completed successfully")
            exit(0)
        else:
            logger.error("Verification failed")
            exit(1)
    except Exception as e:
        logger.error(f"Verification error: {str(e)}")
        exit(1)
    finally:
        verifier.close()

if __name__ == '__main__':
    main()