#!/usr/bin/env python3
"""
Multi-Schema Data Fetcher
Retrieves data from multiple database schemas using credentials from a control database.
"""

import pymysql
import psycopg2
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Any
import logging
from datetime import datetime
import csv
import os
import json
import threading
import argparse

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'schema_fetch_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class MultiSchemaFetcher:
    """Fetches data from multiple database schemas."""
    
    def __init__(self, meta_db_config: Dict[str, Any], max_workers: int = 10):
        """
        Initialize the fetcher.
        
        Args:
            meta_db_config: Configuration for the meta control database
            max_workers: Maximum number of concurrent connections
        """
 
        self.meta_db_config = meta_db_config
        self.max_workers = max_workers 
        self.results = []
        self.errors = []
        self.active_threads = {}  # Track active thread work
        self.thread_lock = threading.Lock()
        self.stop_progress_timer = False
    
    def get_schema_credentials(self) -> List[Dict[str, str]]:
        """
        Retrieve credentials for all active and installed schemas.
        
        Returns:
            List of dictionaries containing credentials for each schema
        """
        logger.info("Fetching schema credentials from meta database...")
        
        query = """
            SELECT 
                database_password, 
                database_name, 
                database_user, 
                database_servers.name as server_name
            FROM instances 
            JOIN database_servers ON instances.database_server_id = database_servers.replica_master_id 
            WHERE is_active = 1 AND is_installed = 1 and instances.id in (408520, 611219)
        """
        
        try:
            conn = pymysql.connect(**self.meta_db_config['params'])
            
            cursor = conn.cursor()
            cursor.execute(query)
            
            columns = [desc[0] for desc in cursor.description]
            credentials = []
            
            for row in cursor.fetchall():
                cred = dict(zip(columns, row))
                credentials.append(cred)
            
            cursor.close()
            conn.close()
            
            logger.info(f"Retrieved credentials for {len(credentials)} schemas")
            return credentials
            
        except Exception as e:
            logger.error(f"Error fetching credentials: {e}")
            raise
    
    def fetch_from_schema(self, credentials: Dict[str, str], query: str) -> Dict[str, Any]:
        """
        Fetch data from a single schema.
        
        Args:
            credentials: Database credentials
            query: SQL query to execute
            
        Returns:
            Dictionary containing schema name and results
        """
        schema_name = credentials['database_name']
        thread_name = f"Worker-{schema_name}"
        connection_timeout = 15  # seconds
        
        try:
            # Update thread status
            with self.thread_lock:
                self.active_threads[thread_name] = {
                    'schema': schema_name,
                    'status': 'connecting',
                    'start_time': datetime.now().isoformat()
                }
            
            # Connect to the schema with timeout
            try:
                conn = pymysql.connect(
                    host=credentials['server_name'],
                    user=credentials['database_user'],
                    password=credentials['database_password'],
                    database=credentials['database_name'],
                    connect_timeout=connection_timeout
                )
            except (pymysql.err.OperationalError, pymysql.err.Error) as e:
                # Connection timeout or failure
                logger.error(f"Connection timeout for {schema_name} after {connection_timeout}s: {e}")
                with self.thread_lock:
                    if thread_name in self.active_threads:
                        del self.active_threads[thread_name]
                
                return {
                    'schema': schema_name,
                    'status': 'error',
                    'error': f'Connection timeout after {connection_timeout}s: {str(e)}'
                }
            
            # Update status
            with self.thread_lock:
                self.active_threads[thread_name]['status'] = 'querying'
            
            cursor = conn.cursor()
            cursor.execute(query)
            
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            
            # Convert to list of dictionaries
            data = [dict(zip(columns, row)) for row in rows]
            
            cursor.close()
            conn.close()
            
            logger.info(f"Successfully fetched data from {schema_name}: {len(data)} rows")
            
            # Clean up thread status
            with self.thread_lock:
                if thread_name in self.active_threads:
                    del self.active_threads[thread_name]
            
            return {
                'schema': schema_name,
                'status': 'success',
                'row_count': len(data),
                'data': data
            }
            
        except Exception as e:
            logger.error(f"Error fetching from {schema_name}: {e}")
            
            # Clean up thread status
            with self.thread_lock:
                if thread_name in self.active_threads:
                    del self.active_threads[thread_name]
            
            return {
                'schema': schema_name,
                'status': 'error',
                'error': str(e)
            }
    
    def _start_progress_timer(self, interval: int = 5):
        """Start a background thread to write progress file periodically.
        
        Args:
            interval: Write progress file every N seconds
        """
        def timer_thread():
            while not self.stop_progress_timer:
                try:
                    self._write_progress_file()
                except Exception as e:
                    logger.error(f"Error in progress timer: {e}")
                
                # Sleep in small increments to allow quick shutdown
                for _ in range(interval * 10):
                    if self.stop_progress_timer:
                        break
                    import time
                    time.sleep(0.1)
        
        thread = threading.Thread(target=timer_thread, daemon=True)
        thread.start()
        return thread
    
    def _write_progress_file(self):
        """Write current active threads to progress file."""
        try:
            with self.thread_lock:
                progress_data = {
                    'timestamp': datetime.now().isoformat(),
                    'active_threads': len(self.active_threads),
                    'threads': self.active_threads.copy()
                }
            
            with open('thread_progress.json', 'w') as f:
                json.dump(progress_data, f, indent=2)
        except Exception as e:
            logger.error(f"Error writing progress file: {e}")
    
    def fetch_all_schemas(self, query: str, group_column: str = None, output_dir: str = '.', single_file_name: str = None) -> Dict[str, Any]:
        """
        Fetch data from all schemas concurrently and stream directly to CSV files.
        
        Args:
            query: SQL query to execute on each schema
            group_column: Column name to group by for file naming (optional)
            output_dir: Directory to save CSV files to
            
        Returns:
            Dictionary containing results statistics
        """
        credentials_list = self.get_schema_credentials()
        
        logger.info(f"Starting parallel fetch from {len(credentials_list)} schemas...")
        logger.info(f"Using {self.max_workers} concurrent workers")
        
        os.makedirs(output_dir, exist_ok=True)
        
        # Thread-safe file writers
        import threading
        lock = threading.Lock()
        group_writers = {}
        group_fieldnames = {}
        group_row_counts = {}
        open_files = {}
        
        def write_row(schema_name: str, data_row: Dict):
            """Thread-safe row writer."""
            nonlocal group_row_counts
            
            row = {'schema': schema_name}
            row.update(data_row)
            # If a single output filename is requested, force a single group.
            if single_file_name:
                group_value = 'all'
            else:
                if group_column:
                    group_value = data_row.get(group_column, 'unknown')
                else:
                    group_value = 'all'
            
            with lock:
                # Initialize writer for this group on first row
                if group_value not in group_writers:
                    if single_file_name:
                        filename = single_file_name
                    else:
                        if group_column:
                            filename = f"{group_column}_{group_value}.csv"
                        else:
                            filename = 'schema_results.csv'
                    
                    output_file = os.path.join(output_dir, filename)
                    open_files[group_value] = open(output_file, 'w', newline='')
                    group_row_counts[group_value] = 0
                
                # Determine fieldnames on first row of each group
                if group_value not in group_fieldnames:
                    fieldnames = ['schema'] + sorted([k for k in row.keys() if k != 'schema'])
                    group_fieldnames[group_value] = fieldnames
                    group_writers[group_value] = csv.DictWriter(
                        open_files[group_value],
                        fieldnames=fieldnames,
                        extrasaction='ignore'
                    )
                    group_writers[group_value].writeheader()
                
                # Write row immediately
                group_writers[group_value].writerow(row)
                group_row_counts[group_value] += 1
                
                # Log progress
                total_written = sum(group_row_counts.values())
                if total_written % 10000 == 0:
                    logger.info(f"Written {total_written} rows so far...")
        
        start_time = datetime.now()
        success_count = 0
        error_count = 0
        
        # Start background progress timer
        progress_thread = self._start_progress_timer(interval=5)
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all tasks
            future_to_schema = {
                executor.submit(self.fetch_from_schema, creds, query): creds['database_name']
                for creds in credentials_list
            }
            
            # Process completed tasks
            for future in as_completed(future_to_schema):
                schema_name = future_to_schema[future]
                try:
                    result = future.result()
                    
                    if result['status'] == 'success':
                        success_count += 1
                        # Stream write each row immediately
                        for data_row in result.get('data', []):
                            write_row(schema_name, data_row)
                    else:
                        error_count += 1
                        self.errors.append(result)
                        
                except Exception as e:
                    logger.error(f"Unexpected error for {schema_name}: {e}")
                    error_count += 1
                    self.errors.append({
                        'schema': schema_name,
                        'status': 'error',
                        'error': str(e)
                    })
        
        # Close all files
        with lock:
            for f in open_files.values():
                f.close()
        
        # Stop progress timer
        self.stop_progress_timer = True
        # Write final progress
        self._write_progress_file()
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        # Log final results
        logger.info(f"Fetch completed in {duration:.2f} seconds")
        logger.info(f"Success: {success_count}, Failed: {error_count}")
        total_rows = sum(group_row_counts.values())
        logger.info(f"Total rows written: {total_rows}")
        for group_value, count in sorted(group_row_counts.items()):
            logger.info(f"  Group {group_value}: {count} rows")
        
        summary = {
            'total_schemas': len(credentials_list),
            'successful': success_count,
            'failed': error_count,
            'duration_seconds': duration,
            'total_rows_written': total_rows,
            'errors': self.errors
        }
        
        return summary
    
    def save_results(self, summary: Dict[str, Any], group_column: str = None, output_dir: str = '.', single_file_name: str = None):
        """Save results to CSV files, streaming data to avoid memory issues.
        
        Args:
            summary: Results summary from fetch_all_schemas
            group_column: Column name to group by for file naming (optional)
            output_dir: Directory to save CSV files to
        """
        try:
            results = summary.get('results', [])
            
            if not results:
                logger.warning("No results to save")
                return
            
            os.makedirs(output_dir, exist_ok=True)
            
            # If grouping, keep writers open per group
            group_writers = {}
            group_fieldnames = {}
            group_row_counts = {}
            open_files = {}
            
            row_count = 0
            
            for result in results:
                schema_name = result.get('schema', '')
                
                if result.get('status') == 'success' and 'data' in result:
                    for data_row in result['data']:
                        row = {'schema': schema_name}
                        row.update(data_row)
                        
                        # Determine group if specified, or force single file
                        if single_file_name:
                            group_value = 'all'
                        else:
                            if group_column:
                                group_value = data_row.get(group_column, 'unknown')
                            else:
                                group_value = 'all'
                        
                        # Initialize writer for this group on first row
                        if group_value not in group_writers:
                            if single_file_name:
                                filename = single_file_name
                            else:
                                if group_column:
                                    filename = f"{group_column}_{group_value}.csv"
                                else:
                                    filename = 'schema_results.csv'
                            
                            output_file = os.path.join(output_dir, filename)
                            open_files[group_value] = open(output_file, 'w', newline='')
                            group_row_counts[group_value] = 0
                        
                        # Determine fieldnames on first row of each group
                        if group_value not in group_fieldnames:
                            fieldnames = ['schema'] + sorted([k for k in row.keys() if k != 'schema'])
                            group_fieldnames[group_value] = fieldnames
                            group_writers[group_value] = csv.DictWriter(
                                open_files[group_value],
                                fieldnames=fieldnames,
                                extrasaction='ignore'
                            )
                            group_writers[group_value].writeheader()
                        
                        # Write row immediately
                        group_writers[group_value].writerow(row)
                        group_row_counts[group_value] += 1
                        row_count += 1
                        
                        # Log progress every 10k rows
                        if row_count % 10000 == 0:
                            logger.info(f"Written {row_count} rows so far...")
            
            # Close all files
            for f in open_files.values():
                f.close()
            
            # Log final results
            logger.info(f"Total rows written: {row_count}")
            for group_value, count in sorted(group_row_counts.items()):
                logger.info(f"  Group {group_value}: {count} rows")
                
        except Exception as e:
            logger.error(f"Error saving results: {e}")


def main():
    """Main execution function."""
    
    # Configuration for the meta control database
    meta_db_config = {
        'type': 'mysql',
        'params': {
            'host': os.getenv('MYSQL_HOST', 'localhost'),
            'user': os.getenv('MYSQL_DB_USER'),
            'password': os.getenv('MYSQL_DB_PASSWORD'),
            'database': 'testrail_meta',
            'port': int(os.getenv('MYSQL_DB_PORT', 3306))
        }
    }
    # The query you want to run on each schema. You can pass a custom SQL
    # via `--query`, or use `--complex` to run the predefined complex query.
    # The original query is preserved below (can be used as default or
    # uncommented if desired):
    # ORIGINAL (simple) QUERY:
    # SELECT is_copy as copy, COUNT(*) as count from cases GROUP BY is_copy

    parser = argparse.ArgumentParser(description='Fetch data from multiple schemas')
    parser.add_argument('--query', help='SQL query to run on each schema')
    parser.add_argument('--complex', action='store_true', help='Run predefined complex query (tests executed by year)')
    parser.add_argument('--workers', type=int, default=10, help='Max concurrent workers')
    parser.add_argument('--group-column', default=None, help='Column name to group CSV outputs by (default: copy, or year when using --complex)')
    parser.add_argument('--output-dir', default='.', help='Directory to save CSV files')
    args = parser.parse_args()

    # Predefined complex query (counts tests executed per year across test_changes)
    complex_query = """
        SELECT YEAR(FROM_UNIXTIME(created_on)) AS year,
               COUNT(*) AS tests_executed
        FROM test_changes
        GROUP BY year
        ORDER BY year
    """

    # Choose query: explicit `--query` > `--complex` > default simple query
    if args.query:
        data_query = args.query
    elif args.complex:
        data_query = complex_query
    else:
        data_query = """
        SELECT is_copy as copy, COUNT(*) as count from cases GROUP BY is_copy
    """

    # Determine effective group column: prefer explicit arg, otherwise
    # default to `year` for the complex query and `copy` for the simple one.
    # Also, if running the complex query without an explicit group column,
    # write a single output file named `complex.csv` instead of per-group files.
    single_file_name = None
    if args.group_column is not None:
        effective_group_column = args.group_column
    else:
        if args.complex:
            # Use a single file for the predefined complex query
            effective_group_column = None
            single_file_name = 'complex.csv'
        else:
            effective_group_column = 'copy'
    
    # Initialize fetcher with requested concurrent workers
    fetcher = MultiSchemaFetcher(meta_db_config, max_workers=args.workers)

    # Fetch data from all schemas and stream directly to CSV
    summary = fetcher.fetch_all_schemas(data_query, group_column=effective_group_column, output_dir=args.output_dir, single_file_name=single_file_name)
    
    # Print summary
    print(f"\n{'='*60}")
    print(f"SUMMARY")
    print(f"{'='*60}")
    print(f"Total Schemas: {summary['total_schemas']}")
    print(f"Successful: {summary['successful']}")
    print(f"Failed: {summary['failed']}")
    print(f"Duration: {summary['duration_seconds']:.2f} seconds")
    print(f"{'='*60}\n")
    
    # Print first few errors if any
    if summary['errors']:
        print("First 5 errors:")
        for error in summary['errors'][:5]:
            print(f"  - {error['schema']}: {error['error']}")


if __name__ == '__main__':
    main()
