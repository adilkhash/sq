# SQS Jobs

A Python job queue library that mirrors python-rq's API but uses Amazon SQS instead of Redis as the message broker.

## Features

- **Familiar API**: Mirrors python-rq's API for easy migration
- **SQS Backend**: Uses Amazon SQS for reliable message queuing
- **JSON Serialization**: Secure JSON-only serialization with automatic type conversion
- **Process Pool Execution**: Parallel job processing with configurable worker processes
- **FIFO Support**: Support for both Standard and FIFO SQS queues
- **Pluggable Result Storage**: Optional result backends (Memory, S3, Redis)
- **Graceful Shutdown**: Coordinated worker shutdown with configurable grace periods
- **Timeout Handling**: Per-job timeout configuration with soft termination
- **Error Handling**: Built-in retry mechanism using SQS's redelivery features

## Installation

```bash
pip install sqs-jobs
```

## Quick Start

```python
import boto3
from sqs_jobs import Queue, Worker, job

# Set up SQS client
sqs_client = boto3.client('sqs', region_name='us-east-1')

# Define a job
@job(timeout=300)
def add_numbers(x, y):
    return x + y

# Create queue and enqueue job
queue = Queue('my-queue', sqs_client)
job = queue.enqueue(add_numbers, 5, 3)

# Process jobs with worker
worker = Worker([queue], sqs_client)
worker.work()  # Starts processing jobs
```

## Core Components

### Queue

The `Queue` class handles job enqueueing and SQS queue management.

```python
from sqs_jobs import Queue

# Standard queue
queue = Queue('my-queue', sqs_client)

# FIFO queue for ordered processing
fifo_queue = Queue('my-fifo-queue', sqs_client, queue_type='fifo')

# Queue with custom settings
queue = Queue(
    'my-queue',
    sqs_client,
    visibility_timeout=1800  # 30 minutes
)
```

### Worker

The `Worker` class processes jobs from one or more queues using a process pool.

```python
from sqs_jobs import Worker

# Single queue worker
worker = Worker([queue], sqs_client)

# Multi-queue worker with custom settings
worker = Worker(
    [queue1, queue2],
    sqs_client,
    num_processes=4,      # Number of worker processes
    grace_period=60,      # Shutdown grace period in seconds
    result_backend=result_backend  # Optional result storage
)

# Start processing
worker.work()
```

### Job Decorator

Use the `@job` decorator to configure job-specific settings.

```python
from sqs_jobs import job

@job(timeout=300)  # 5 minute timeout
def my_task(data):
    # Process data
    return result

# Job without decorator (uses defaults)
def simple_task(x, y):
    return x + y
```

## Serialization

SQS Jobs uses JSON serialization with automatic type conversion for:

- `datetime` → ISO format string
- `date` → ISO format string  
- `time` → ISO format string
- `Decimal` → string representation
- `UUID` → string representation
- `bytes` → base64 encoded string
- `set` → list
- `tuple` → list

```python
from datetime import datetime
from decimal import Decimal
from uuid import uuid4

# These types are automatically serialized/deserialized
job = queue.enqueue(
    my_task,
    datetime.now(),
    Decimal('123.45'),
    uuid4(),
    {'key': 'value'}
)
```

## Result Backends

SQS Jobs supports pluggable result storage backends.

### Memory Backend

```python
from sqs_jobs.result_backend import MemoryResultBackend

backend = MemoryResultBackend(ttl=3600)  # 1 hour TTL
worker = Worker([queue], sqs_client, result_backend=backend)
```

### S3 Backend

```python
from sqs_jobs.result_backend import S3ResultBackend

s3_client = boto3.client('s3')
backend = S3ResultBackend(s3_client, 'my-results-bucket')
worker = Worker([queue], sqs_client, result_backend=backend)
```

### Redis Backend

```python
from sqs_jobs.result_backend import RedisResultBackend
import redis

redis_client = redis.Redis(host='localhost', port=6379, db=0)
backend = RedisResultBackend(redis_client, ttl=3600)
worker = Worker([queue], sqs_client, result_backend=backend)
```

### Custom Backend

```python
from sqs_jobs.result_backend import ResultBackend

class CustomResultBackend(ResultBackend):
    def store(self, job_id: str, result: Any) -> None:
        # Store result
        pass
    
    def get(self, job_id: str) -> Any:
        # Retrieve result
        pass
```

## Error Handling

SQS Jobs leverages SQS's built-in retry and dead letter queue features:

1. **Automatic Retries**: Failed jobs are automatically retried based on SQS queue configuration
2. **Dead Letter Queues**: Messages that fail repeatedly are moved to a dead letter queue
3. **Visibility Timeout**: Failed jobs become visible again after the timeout expires
4. **Graceful Degradation**: Worker continues processing other jobs if one fails

## Configuration

### AWS Configuration

```python
import boto3

# Explicit configuration
sqs_client = boto3.client(
    'sqs',
    region_name='us-east-1',
    aws_access_key_id='your-key',
    aws_secret_access_key='your-secret'
)

# Using IAM roles or environment variables
sqs_client = boto3.client('sqs', region_name='us-east-1')
```

### Queue Configuration

```python
# Standard queue with custom visibility timeout
queue = Queue(
    'my-queue',
    sqs_client,
    visibility_timeout=1800  # 30 minutes
)

# FIFO queue
fifo_queue = Queue(
    'my-fifo-queue',
    sqs_client,
    queue_type='fifo'
)
```

### Worker Configuration

```python
worker = Worker(
    queues=[queue1, queue2],
    sqs_client=sqs_client,
    num_processes=4,      # Number of worker processes
    grace_period=30,      # Graceful shutdown timeout
    result_backend=backend  # Optional result storage
)
```

## Examples

See the `examples/` directory for complete usage examples:

- `basic_usage.py` - Basic queue and worker setup
- `worker_example.py` - Multi-queue worker with signal handling
- `fifo_queue_example.py` - FIFO queue for ordered processing

## Development

```bash
# Install development dependencies
pip install -r requirements-dev.txt

# Run tests
pytest tests/

# Run with coverage
pytest tests/ --cov=sqs_jobs
```

## License

MIT License - see LICENSE file for details.