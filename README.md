# Fletch

Fletch is a service that connects Apache Arrow Flight with Qdrant, a vector database. It enables storing Arrow data in Qdrant and querying Qdrant using vector similarity search, returning results in Arrow format.

## Features

- **Apache Arrow Integration**: Seamlessly integrate with any system that works with Arrow data format.
- **Vector Similarity Search**: Leverage Qdrant's efficient vector search capabilities.
- **Batch Processing**: Process large datasets in configurable batches to optimize memory usage and throughput.
- **Progress Reporting**: Track the progress of batch operations with real-time updates.
- **Schema Management**: Automatic schema inference and validation.
- **Collection Management**: Create, list, check existence, and delete Qdrant collections.

## Configuration

Fletch uses a flexible configuration system that supports file-based and environment variable configuration.

Example configuration:

```yaml
server:
  host: localhost
  port: 8815
  tls_enabled: false
  max_concurrent: 10

qdrant:
  host: localhost
  port: 6334
  use_tls: false
  api_key: ""
  pool_size: 10
  batch_size: 100  # Size for batch processing

log:
  level: info
  format: json
```

## Batch Processing

Fletch supports efficient batch processing for large datasets. Instead of loading the entire dataset into memory, it processes data in smaller configurable batches, which is crucial for production workloads with limited resources.

### Batch Size Configuration

You can configure the batch size in two ways:

1. In your configuration file or environment variables:

   ```yaml
   qdrant:
     batch_size: 100  # Process 100 records at a time
   ```

2. When making a DoPut request:

   ```json
   {
     "collection_name": "my_collection",
     "batch_size": 200,
     "report_progress": true
   }
   ```

## Progress Reporting

For long-running batch operations, Fletch provides progress reporting to monitor the status:

1. Enable progress reporting in your DoPut request:

   ```json
   {
     "collection_name": "my_collection",
     "report_progress": true
   }
   ```

2. The server logs progress updates with:
   - Number of processed records
   - Total records
   - Percentage complete

## Usage Examples

### Storing Data in Qdrant

```python
import pyarrow as pa
import pyarrow.flight as flight
import json

# Create a Flight client
client = flight.FlightClient("grpc://localhost:8815")

# Prepare your Arrow data
data = pa.RecordBatch.from_arrays(
    [
        pa.array(["id1", "id2"]),
        pa.array([[1.0, 2.0, 3.0], [4.0, 5.0, 6.0]]),
        pa.array(["metadata1", "metadata2"])
    ],
    names=["id", "vector", "metadata"]
)

# Create a descriptor with batch processing options
options = {
    "collection_name": "my_collection",
    "create_collection": True,
    "batch_size": 100,
    "report_progress": True
}
descriptor = flight.FlightDescriptor.for_command(json.dumps(options).encode())

# Upload the data
writer, _ = client.do_put(descriptor)
writer.write_batch(data)
writer.close()
```

### Searching Data

```python
import pyarrow.flight as flight
import json

# Create a Flight client
client = flight.FlightClient("grpc://localhost:8815")

# Define search parameters
search_params = {
    "collection_name": "my_collection",
    "vector": [1.0, 2.0, 3.0],
    "limit": 10
}

# Create a ticket with the search parameters
ticket = flight.Ticket(json.dumps(search_params).encode())

# Perform the search
reader = client.do_get(ticket)
for batch in reader:
    # Process the results
    print(batch.data.to_pandas())
```

## License

[MIT](LICENSE)
