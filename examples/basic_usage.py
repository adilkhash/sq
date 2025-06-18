#!/usr/bin/env python3
"""
Basic usage example for sqs-jobs library.
This demonstrates how to set up queues, enqueue jobs, and process them with workers.
"""

import boto3
from sqs_jobs import Queue, Worker, job
from sqs_jobs.result_backend import MemoryResultBackend
import requests


@job(timeout=300)
def add_numbers(x, y):
    """Simple addition task."""
    print(f"Adding {x} + {y}")
    return x + y


@job(timeout=60)
def multiply_numbers(x, y):
    """Simple multiplication task."""
    r = requests.get("http://khashtamov.com/").text
    return r


def process_data(data_list):
    """Process a list of data."""
    print(f"Processing {len(data_list)} items")
    processed = [item * 2 for item in data_list]
    return {"processed": processed, "count": len(processed)}


def main():
    # Set up AWS SQS client (you'll need proper AWS credentials)
    sqs_client = boto3.client("sqs", region_name="eu-central-1")

    # Optional: Set up result backend for job results
    result_backend = MemoryResultBackend(ttl=3600)  # 1 hour TTL

    # Create a queue
    queue = Queue("queue-name", sqs_client)

    # Enqueue some jobs
    print("Enqueuing jobs...")

    job1 = queue.enqueue(add_numbers, 5, 3)
    print(f"Enqueued job {job1.job_id}: add_numbers(5, 3)")

    job2 = queue.enqueue(multiply_numbers, 4, 7)
    print(f"Enqueued job {job2.job_id}: multiply_numbers(4, 7)")

    job3 = queue.enqueue(process_data, [1, 2, 3, 4, 5])
    print(f"Enqueued job {job3.job_id}: process_data([1, 2, 3, 4, 5])")

    # Set up worker to process jobs
    print("\nStarting worker...")
    worker = Worker(
        [queue],
        sqs_client,
        num_processes=2,
        grace_period=30,
        result_backend=result_backend,
    )

    # In a real application, you would call worker.work() to start processing

    print("Jobs enqueued successfully!")
    print("To process jobs, run the worker with worker.work()")

    try:
        worker.work()
    except KeyboardInterrupt:
        print("\nShutting down...")
        worker.shutdown()


if __name__ == "__main__":
    main()
