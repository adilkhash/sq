# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Commands

### Environment Setup
```bash
# Create and activate virtual environment
python3 -m venv .venv
source .venv/bin/activate  # Linux/Mac
# or .venv\Scripts\activate  # Windows

# Install development dependencies
pip install -r requirements-dev.txt
```

### Testing
```bash
# Run all tests
pytest tests/

# Run tests with coverage
pytest tests/ --cov=sqs_jobs

# Run specific test file
pytest tests/test_queue.py

# Run specific test method
pytest tests/test_queue.py::TestQueue::test_enqueue_job

# Run tests with verbose output
pytest tests/ -v

# Run integration tests only
pytest tests/test_integration.py
```

### Package Management
```bash
# Install package in development mode
pip install -e .

# Install with dev dependencies
pip install -e .[dev]

# Build package
python setup.py sdist bdist_wheel
```

## Architecture Overview

This is a Python job queue library that mirrors python-rq's API but uses Amazon SQS as the message broker instead of Redis. The architecture follows a clean separation of concerns:

### Core Components

**Queue (`queue.py`)**: Manages SQS queue lifecycle, job enqueueing, and message handling. Supports both Standard and FIFO SQS queues with automatic queue creation and retry logic.

**Worker (`worker.py`)**: Implements process pool execution model for job processing. Handles multiple queues, graceful shutdown, signal handling, and timeout management using `ProcessPoolExecutor`.

**Job (`job.py`)**: Represents individual jobs with metadata, serialization, and execution logic. Includes the `@job` decorator for timeout and result backend configuration.

**Serializer (`serializer.py`)**: Handles JSON serialization with custom type conversion for datetime, Decimal, UUID, bytes, sets, and tuples. Critical for SQS message format compatibility.

**Result Backends (`result_backend.py`)**: Pluggable storage system with abstract base class and implementations for Memory, S3, and Redis storage.

### Message Flow Architecture

1. **Job Enqueueing**: Queue serializes job data to JSON, adds SQS message attributes (job_id, function_name, timestamps), and sends to SQS with retry logic
2. **Job Processing**: Worker polls SQS, deserializes jobs, executes in separate processes with timeout handling, and deletes successful messages
3. **Error Handling**: Uses SQS's native redelivery mechanism and Dead Letter Queues rather than application-level retry tracking

### Key Design Patterns

- **Process Isolation**: Each job runs in a separate process via `multiprocessing.ProcessPoolExecutor`
- **SQS-Native Features**: Leverages visibility timeout, DLQ, and FIFO ordering rather than reimplementing these features
- **Pluggable Backends**: Result storage follows abstract interface pattern for easy extension
- **Signal-Based Shutdown**: Graceful worker termination using SIGTERM/SIGINT with configurable grace periods

### Testing Strategy

- **Mocked SQS**: Uses `moto` library to mock AWS SQS for unit tests
- **Fixtures**: `conftest.py` provides reusable SQS client and result backend fixtures
- **Integration Tests**: End-to-end scenarios in `test_integration.py` with real SQS simulation
- **Component Tests**: Individual module testing with comprehensive edge cases

### Configuration Requirements

- **AWS Credentials**: Requires explicit boto3 SQS client configuration - no implicit credential discovery
- **Queue Types**: Supports 'standard' and 'fifo' queue types with different SQS configurations
- **Timeouts**: Configurable at job level (decorator), queue level (visibility timeout), and worker level (grace period)

This architecture prioritizes reliability and AWS integration over complex features, following the unix philosophy of doing one thing well.