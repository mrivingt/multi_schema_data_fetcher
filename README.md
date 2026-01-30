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

Edit the `main()` function in `multi_schema_data_fetcher.py`:

### 1. Configure Meta Database Connection

```python
meta_db_config = {
    'type': 'mysql',  # or 'postgres'
    'params': {
        'host': 'your_meta_db_host',
        'user': 'your_meta_db_user',
        'password': 'your_meta_db_password',
        'database': 'meta',
        'port': 3306
    }
}
```

### 2. Define Your Data Query

```python
data_query = """
    SELECT * FROM your_table_name LIMIT 100
"""
```

### 3. Adjust Concurrency

```python
# Adjust max_workers based on your database server capacity
fetcher = MultiSchemaFetcher(meta_db_config, max_workers=50)
```

**Recommended max_workers settings:**
- For 10,000 schemas: Start with 50-100 workers
- Monitor database server load and adjust accordingly
- More workers = faster completion but higher load

## Usage

```bash
python multi_schema_data_fetcher.py
```

## Output

The script generates:

1. **Log file**: `schema_fetch_YYYYMMDD_HHMMSS.log` - Detailed execution log
2. **Results file**: `schema_results.json` - JSON file containing:
   - Success/failure counts
   - Data from each schema
   - Error details for failed fetches
   - Execution statistics

### Sample Output Structure

```json
{
  "total_schemas": 10000,
  "successful": 9950,
  "failed": 50,
  "duration_seconds": 245.67,
  "results": [
    {
      "schema": "database_1",
      "status": "success",
      "row_count": 100,
      "data": [...]
    }
  ],
  "errors": [
    {
      "schema": "database_2",
      "status": "error",
      "error": "Connection timeout"
    }
  ]
}
```

## Performance Considerations

### For 10,000 Schemas:

**Estimated time with different worker counts:**
- 10 workers: ~2.8 hours (assuming 1 second per schema)
- 50 workers: ~33 minutes
- 100 workers: ~17 minutes
- 200 workers: ~8.5 minutes

**Recommendations:**
1. Start with 50 workers and monitor performance
2. Increase gradually if database servers can handle the load
3. Consider database connection limits
4. Monitor network bandwidth and latency

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

## Troubleshooting

### Issue: Too many connections error

**Solution**: Reduce `max_workers` value

### Issue: Slow performance

**Solutions**:
- Increase `max_workers` if database can handle it
- Optimize your data query
- Use `LIMIT` clauses to reduce data transfer
- Check network latency

### Issue: Memory issues

**Solutions**:
- Process results in batches
- Don't load all data into memory at once
- Stream results to disk

### Issue: Connection timeouts

**Solutions**:
- Increase `connect_timeout` in `fetch_from_schema()`
- Check network connectivity
- Verify database server is responsive

## Advanced Usage

### Batch Processing

If you need to process schemas in batches:

```python
credentials_list = fetcher.get_schema_credentials()
batch_size = 1000

for i in range(0, len(credentials_list), batch_size):
    batch = credentials_list[i:i+batch_size]
    # Process batch
```

### Different Queries per Schema

Modify `fetch_from_schema()` to accept custom queries or use schema-specific logic.

## Security Notes

- Store database credentials securely (use environment variables or secret management)
- Use read-only database users when possible
- Implement connection pooling for production use
- Consider using SSL/TLS for database connections

## License

MIT License - feel free to modify and use as needed.
