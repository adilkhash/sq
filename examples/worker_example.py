#!/usr/bin/env python3
"""
Worker example for sqs-jobs library.
This demonstrates how to set up a worker to process jobs from multiple queues.
"""

import boto3
import signal
import sys
from sqs_jobs import Queue, Worker
from sqs_jobs.result_backend import MemoryResultBackend


def setup_signal_handlers(worker):
    """Set up signal handlers for graceful shutdown."""
    def signal_handler(signum, frame):
        print(f"\nReceived signal {signum}, shutting down worker...")
        worker.shutdown()
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)


def main():
    # Set up AWS SQS client
    sqs_client = boto3.client('sqs', region_name='us-east-1')
    
    # Set up result backend
    result_backend = MemoryResultBackend(ttl=3600)
    
    # Create multiple queues to process
    high_priority_queue = Queue('high-priority', sqs_client)
    normal_queue = Queue('normal-jobs', sqs_client)
    batch_queue = Queue('batch-processing', sqs_client)
    
    # Create worker with multiple queues
    worker = Worker(
        queues=[high_priority_queue, normal_queue, batch_queue],
        sqs_client=sqs_client,
        num_processes=4,  # Use 4 worker processes
        grace_period=60,  # Wait up to 60 seconds for jobs to complete during shutdown
        result_backend=result_backend
    )
    
    # Set up signal handlers for graceful shutdown
    setup_signal_handlers(worker)
    
    print("Starting worker with the following queues:")
    for queue in worker.queues:
        print(f"  - {queue.name} ({queue.queue_type})")
    
    print(f"Worker processes: {worker.num_processes}")
    print(f"Grace period: {worker.grace_period} seconds")
    print("\nWorker is running... Press Ctrl+C to stop gracefully.")
    
    try:
        # Start processing jobs
        worker.work()
    except KeyboardInterrupt:
        print("\nShutting down...")
        worker.shutdown()


if __name__ == "__main__":
    main()