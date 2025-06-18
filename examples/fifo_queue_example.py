#!/usr/bin/env python3
"""
FIFO queue example for sqs-jobs library.
This demonstrates how to use FIFO queues for ordered job processing.
"""

import boto3
from sqs_jobs import Queue, Worker, job


@job(timeout=120)
def process_order(order_id, customer_id, items):
    """Process a customer order."""
    print(f"Processing order {order_id} for customer {customer_id}")
    print(f"Items: {items}")

    # Simulate order processing
    total_amount = sum(item["price"] * item["quantity"] for item in items)

    result = {
        "order_id": order_id,
        "customer_id": customer_id,
        "total_amount": total_amount,
        "status": "processed",
    }

    print(f"Order {order_id} processed successfully. Total: ${total_amount:.2f}")
    return result


@job(timeout=60)
def send_notification(customer_id, message):
    """Send notification to customer."""
    print(f"Sending notification to customer {customer_id}: {message}")
    return {"sent": True, "customer_id": customer_id}


def main():
    # Set up AWS SQS client
    sqs_client = boto3.client("sqs", region_name="us-east-1")

    # Create FIFO queue for order processing
    # FIFO queues ensure messages are processed in the exact order they were sent
    order_queue = Queue(
        "order-processing",
        sqs_client,
        queue_type="fifo",  # Use FIFO queue
        visibility_timeout=1800,  # 30 minutes
    )

    # Create notification queue (standard queue is fine for notifications)
    notification_queue = Queue("notifications", sqs_client)

    print("Enqueuing orders in FIFO queue...")

    # Sample orders - these will be processed in exact order
    orders = [
        {
            "order_id": "ORDER-001",
            "customer_id": "CUST-123",
            "items": [
                {"name": "Widget A", "price": 10.99, "quantity": 2},
                {"name": "Widget B", "price": 15.50, "quantity": 1},
            ],
        },
        {
            "order_id": "ORDER-002",
            "customer_id": "CUST-124",
            "items": [{"name": "Widget C", "price": 25.00, "quantity": 1}],
        },
        {
            "order_id": "ORDER-003",
            "customer_id": "CUST-123",
            "items": [
                {"name": "Widget A", "price": 10.99, "quantity": 1},
                {"name": "Widget D", "price": 8.75, "quantity": 3},
            ],
        },
    ]

    # Enqueue orders
    for order in orders:
        job = order_queue.enqueue(
            process_order, order["order_id"], order["customer_id"], order["items"]
        )
        print(f"Enqueued order job {job.job_id}: {order['order_id']}")

    # Enqueue some notifications
    notifications = [
        ("CUST-123", "Your order has been received and is being processed."),
        ("CUST-124", "Thank you for your order!"),
        ("CUST-123", "Your order is ready for pickup."),
    ]

    for customer_id, message in notifications:
        job = notification_queue.enqueue(send_notification, customer_id, message)
        print(f"Enqueued notification job {job.job_id} for {customer_id}")

    # Set up worker to process both queues
    worker = Worker([order_queue, notification_queue], sqs_client, num_processes=2)

    print(f"\nCreated worker for queues: {[q.name for q in worker.queues]}")
    print("To start processing, call worker.work()")

    # In production, you would call:
    # worker.work()


if __name__ == "__main__":
    main()
