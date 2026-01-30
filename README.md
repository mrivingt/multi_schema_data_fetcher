# Multi-Schema Data Fetcher

A Python script to efficiently fetch data from 10,000+ database schemas using credentials stored in a central control database.

## Features

- **Concurrent Processing**: Uses ThreadPoolExecutor for parallel database connections
- **Error Handling**: Robust error handling with detailed logging
- **Progress Tracking**: Real-time logging of progress and errors
- **Flexible Configuration**: Easy to configure for different database types and queries
- **Result Persistence**: Saves results to JSON for further processing

## Installation

```bash
pip install -r requirements.txt
```

## Configuration

### 1. export Meta Database Connection ENV Variables

 export META_DB_HOST=production-aurora-cluster.cluster-c6xqywhzdx3n.us-east-1.rds.amazonaws.com
 export META_DB_USER=testrail-meta
 export META_DB_PASSWORD=*********
 export META_DB_USER=testrail_meta


### 2. Define Your Data Query

```python
data_query = """
    SELECT is_copy as copy, COUNT(*) as count from cases GROUP BY is_copy
"""
```

### 3. Adjust Concurrency

```python
# Adjust max_workers based on your database server capacity
fetcher = MultiSchemaFetcher(meta_db_config, max_workers=10)
```
## Usage

```bash
python multi_schema_data_fetcher.py
```

## Output

The script generates:

1. **Log file**: `schema_fetch_YYYYMMDD_HHMMSS.log` - Detailed execution log
2. **Results files**: `schema_results.json` - JSON file containing:
   - Success/failure counts
   - Data from each schema
   - Error details for failed fetches
   - Execution statistics


## Customization Examples

### Example 1: Aggregate Data Collection

```python
data_query = """
    SELECT 
        COUNT(*) as total_records,
        MAX(created_at) as latest_record,
        MIN(created_at) as earliest_record
    FROM your_table
"""
```

### Example 2: Specific Date Range

```python
data_query = """
    SELECT * FROM orders 
    WHERE order_date >= '2024-01-01' 
    AND order_date < '2025-01-01'
"""
```

### Example 3: Processing Results

```python
def process_results(summary):
    """Custom processing of fetched data."""
    for result in summary['results']:
        if result['status'] == 'success':
            schema = result['schema']
            data = result['data']
            # Your custom processing logic here
            print(f"{schema}: {len(data)} records")

# Add to main():
summary = fetcher.fetch_all_schemas(data_query)
process_results(summary)
```

## Error Handling

The script handles common errors:
- Connection timeouts
- Authentication failures
- Query execution errors
- Network issues

All errors are logged with schema name and error details.
