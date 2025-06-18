# SQS Jobs - Python Job Queue Specification

## Overview
A Python job queue library that mirrors python-rq's API but uses Amazon SQS instead of Redis as the message broker.

## Package Structure
- **Package name**: `sqs_jobs`
- **Main exports**: `Queue`, `Worker`, `job` decorator

## Core Features

### 1. API Design
- Mirror python-rq's API for familiarity
- Main interfaces:
  ```python
  from sqs_jobs import Queue, Worker, job
  
  @job
  def my_task(x, y):
      return x + y
  
  q = Queue('default', sqs_client=boto3_client)
  job = q.enqueue(my_task, 1, 2)
  
  worker = Worker([q], sqs_client=boto3_client)
  worker.work()
  ```

### 2. Serialization
- Use JSON format for all job data
- Automatic type conversion for common Python types:
  - `datetime` → ISO format string
  - `date` → ISO format string
  - `time` → ISO format string
  - `Decimal` → string representation
  - `UUID` → string representation
  - `bytes` → base64 encoded string
  - `set` → list
  - `tuple` → list

### 3. Queue Support
- Support both SQS Standard and FIFO queues
- Queue type specified during queue creation
- Named queues with direct SQS queue mapping (e.g., 'default' → 'default' SQS queue)
- No priority system within queues

### 4. Job Execution
- Process pool execution model (like python-rq)
- Configurable number of worker processes
- Single message processing (no batching)
- Immediate execution only (no scheduling/delays)

### 5. Failure Handling
- Use SQS's built-in redelivery mechanism
- Configure max receive count on SQS queue
- Failed messages automatically move to DLQ after max attempts
- No application-level retry tracking

### 6. Result Storage
- Optional pluggable result storage
- Results not stored by default
- Support pluggable backends with interface:
  ```python
  class ResultBackend:
      def store(self, job_id: str, result: Any) -> None: ...
      def get(self, job_id: str) -> Any: ...
  ```

### 7. Timeout Handling
- Soft timeout with grace period
- Send SIGTERM first, then SIGKILL after grace period
- Configurable per job with default fallback
- Implemented at process pool level

### 8. Job Tracking
- Basic tracking using SQS message attributes:
  - Job ID (generated UUID)
  - Function name
  - Enqueue timestamp
  - Queue name
- No persistent job status tracking

### 9. Configuration
- Explicit AWS configuration
- Pass boto3 SQS client to Queue and Worker objects
- No implicit credential discovery
- Example:
  ```python
  sqs_client = boto3.client('sqs', region_name='us-east-1')
  q = Queue('default', sqs_client=sqs_client)
  ```

### 10. Worker Lifecycle
- Coordinated graceful shutdown
- Finish current jobs, stop accepting new ones
- Wait for process pool to drain
- Respond to SIGTERM/SIGINT signals
- Maximum wait time for shutdown

### 11. Error Handling
- Basic error logging to stdout/stderr
- Include job ID and function name in error logs
- Full traceback logged to worker output
- No structured error storage

### 12. Message Configuration
- Queue-level visibility timeout configuration
- Set when creating/configuring queue
- No per-job visibility timeout changes
- No dynamic timeout extension

## Implementation Requirements

### Message Format
SQS message body (JSON):
```json
{
  "job_id": "uuid-string",
  "function": "module.function_name",
  "args": [...],
  "kwargs": {...},
  "enqueued_at": "2024-01-01T00:00:00Z"
}
```

Message attributes:
- `job_id`: String
- `function_name`: String  
- `enqueue_timestamp`: Number (unix timestamp)
- `queue_name`: String

### Queue Operations
1. **Enqueue**: 
   - Serialize job data to JSON
   - Add message attributes
   - Send to appropriate SQS queue

2. **Worker Processing**:
   - Receive single message from SQS
   - Deserialize job data
   - Execute in process pool
   - Delete message on success
   - Let visibility timeout expire on failure

### Key Classes and Methods

```python
class Queue:
    def __init__(self, name: str, sqs_client, queue_type='standard', 
                 visibility_timeout=1800, result_backend=None):
        ...
    
    def enqueue(self, func, *args, **kwargs) -> Job:
        ...

class Worker:
    def __init__(self, queues: List[Queue], sqs_client, 
                 num_processes=None, grace_period=30):
        ...
    
    def work(self):
        ...
    
    def shutdown(self):
        ...

@job(timeout=300, result_backend=None)
def decorated_function():
    ...
```

### Error Scenarios to Handle
1. SQS service errors → retry with exponential backoff
2. Serialization errors → log and skip message
3. Import errors for job function → log and move to DLQ
4. Job timeout → terminate process, let message return to queue
5. Worker shutdown during job → graceful completion

### Dependencies
- `boto3` - AWS SDK
- `multiprocessing` - Process pool
- Standard library only for core functionality

## Design Decisions Summary
1. **JSON only** - No pickle support for security and debugging
2. **SQS-native features** - Leverage DLQ, visibility timeout
3. **Simple job model** - Functions with no special context
4. **Process isolation** - Each job runs in separate process
5. **Explicit configuration** - No magic, user controls AWS setup
6. **python-rq compatibility** - Familiar API for easy migration

## Future Considerations (Not in V1)
- Async job support
- Job chaining/dependencies  
- Advanced scheduling
- Metrics/monitoring integration
- Web UI for job monitoring
