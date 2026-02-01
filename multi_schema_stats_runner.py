#!/usr/bin/env python3
"""Runner to add `--stats` functionality without editing the original file.

This imports `MultiSchemaFetcher` from `multi_schema_data_fetcher`, attaches
`fetch_stats_all_schemas` to the class, and exposes a CLI `--stats` that
writes `schema_stats.csv` to the specified output directory.
"""
import os
import csv
import argparse
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import pymysql

from multi_schema_data_fetcher import MultiSchemaFetcher

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def fetch_stats_all_schemas(self, output_dir='.', output_file='schema_stats.csv'):
    creds_list = self.get_schema_credentials()

    stats_query = """
        SELECT
            SUM(CASE WHEN is_copy=0 THEN 1 ELSE 0 END) AS cases_copy_0,
            SUM(CASE WHEN is_copy=1 THEN 1 ELSE 0 END) AS cases_copy_1,
            (SELECT COUNT(*) FROM tests) AS tests_count,
            (SELECT COUNT(*) FROM test_changes) AS test_changes_count
        FROM cases
    """

    results = []
    errors = []

    def worker(creds):
        schema = creds['database_name']
        try:
            conn = pymysql.connect(
                host=creds['server_name'],
                user=creds['database_user'],
                password=creds['database_password'],
                database=creds['database_name'],
                connect_timeout=15
            )
            cur = conn.cursor()
            cur.execute(stats_query)
            row = cur.fetchone()
            cols = [desc[0] for desc in cur.description]
            cur.close()
            conn.close()

            if row is None:
                rowdata = {c: 0 for c in cols}
            else:
                rowdata = dict(zip(cols, row))

            for k in list(rowdata.keys()):
                try:
                    rowdata[k] = int(rowdata[k]) if rowdata[k] is not None else 0
                except Exception:
                    rowdata[k] = 0

            rowdata['schema'] = schema
            rowdata['sum_tests_and_cases'] = rowdata.get('tests_count', 0) + rowdata.get('cases_copy_0', 0) + rowdata.get('cases_copy_1', 0)
            return {'status': 'success', 'data': rowdata}
        except Exception as e:
            logger.error(f"Error for {schema}: {e}")
            return {'status': 'error', 'error': str(e), 'schema': schema}

    with ThreadPoolExecutor(max_workers=self.max_workers) as ex:
        futures = {ex.submit(worker, c): c['database_name'] for c in creds_list}
        for fut in as_completed(futures):
            res = fut.result()
            if res.get('status') == 'success':
                results.append(res['data'])
            else:
                errors.append(res)

    os.makedirs(output_dir, exist_ok=True)
    outpath = os.path.join(output_dir, output_file)
    fieldnames = ['schema', 'cases_copy_0', 'cases_copy_1', 'tests_count', 'test_changes_count', 'sum_tests_and_cases']
    with open(outpath, 'w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in sorted(results, key=lambda x: x.get('schema')):
            w.writerow({k: r.get(k, 0) for k in fieldnames})

    logger.info(f"Wrote stats for {len(results)} schemas to {outpath}")
    if errors:
        logger.warning(f"{len(errors)} schemas failed; first error: {errors[0]}")

    return {'total': len(creds_list), 'collected': len(results), 'errors': errors, 'output_file': outpath}


# Attach as method
MultiSchemaFetcher.fetch_stats_all_schemas = fetch_stats_all_schemas


def main():
    parser = argparse.ArgumentParser(description='Run multi-schema fetcher with optional stats')
    parser.add_argument('--stats', action='store_true', help='Collect schema stats and write CSV')
    parser.add_argument('--output-dir', default='.', help='Directory for output CSV')
    parser.add_argument('--output-file', default='schema_stats.csv', help='Output CSV filename')
    parser.add_argument('--workers', type=int, default=10, help='Parallel worker count')
    args = parser.parse_args()

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

    fetcher = MultiSchemaFetcher(meta_db_config, max_workers=args.workers)

    if args.stats:
        res = fetcher.fetch_stats_all_schemas(output_dir=args.output_dir, output_file=args.output_file)
        print(res)
    else:
        print('No action specified. Use --stats to collect schema stats.')


if __name__ == '__main__':
    main()
