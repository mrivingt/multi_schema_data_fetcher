#!/usr/bin/env python3
"""Collect per-schema stats and write schema_stats.csv

This script uses `MultiSchemaFetcher.get_schema_credentials()` from
`multi_schema_data_fetcher.py` to obtain schema connections, then runs a
single query on each schema to collect the requested counts and writes
one CSV with one row per schema.
"""

import os
import csv
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
from multi_schema_data_fetcher import MultiSchemaFetcher
import pymysql

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def collect_stats(meta_db_config, output_dir='.', output_file='schema_stats.csv', workers=10):
    fetcher = MultiSchemaFetcher(meta_db_config, max_workers=workers)
    creds_list = fetcher.get_schema_credentials()

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

    with ThreadPoolExecutor(max_workers=workers) as ex:
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


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Collect schema stats and write CSV')
    parser.add_argument('--output-dir', default='.', help='Directory for output CSV')
    parser.add_argument('--output-file', default='schema_stats.csv', help='Output CSV filename')
    parser.add_argument('--workers', type=int, default=10, help='Parallel worker count')
    args = parser.parse_args()

    meta_db_config = {
        'type': 'mysql',
        'params': {
            'host': os.getenv('META_DB_HOST', 'localhost'),
            'user': os.getenv('META_DB_USER'),
            'password': os.getenv('META_DB_PASSWORD'),
            'database': 'testrail_meta',
            'port': int(os.getenv('META_DB_PORT', 3306))
        }
    }

    res = collect_stats(meta_db_config, output_dir=args.output_dir, output_file=args.output_file, workers=args.workers)
    print(res)
