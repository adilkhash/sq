import pytest
import time
import unittest
from unittest.mock import Mock, patch
from moto import mock_aws
import boto3
from sqs_jobs import Queue, Worker, job
from sqs_jobs.result_backend import MemoryResultBackend


@job(timeout=60)
def add_numbers(x, y):
    return x + y


@job(timeout=30)
def multiply_numbers(x, y):
    return x * y


def divide_numbers(x, y):
    if y == 0:
        raise ValueError("Division by zero")
    return x / y


@mock_aws
class TestIntegration(unittest.TestCase):
    def setUp(self):
        self.sqs_client = boto3.client("sqs", region_name="us-east-1")
        self.result_backend = MemoryResultBackend(ttl=3600)

    def test_basic_job_flow(self):
        queue = Queue("test-queue", self.sqs_client)
        
        job = queue.enqueue(add_numbers, 5, 3)
        assert job.job_id is not None
        assert job.function == "tests.test_integration.add_numbers"
        assert job.args == [5, 3]

    def test_job_with_decorated_function(self):
        queue = Queue("test-queue", self.sqs_client)
        
        job = queue.enqueue(add_numbers, 10, 20)
        assert job.timeout == 60

    def test_job_with_kwargs(self):
        queue = Queue("test-queue", self.sqs_client)
        
        job = queue.enqueue(divide_numbers, 10, y=2)
        assert job.args == [10]
        assert job.kwargs == {"y": 2}

    def test_fifo_queue_creation(self):
        queue = Queue("test-fifo", self.sqs_client, queue_type="fifo")
        
        job = queue.enqueue(add_numbers, 1, 2)
        assert job.job_id is not None

    def test_queue_url_caching(self):
        queue = Queue("test-queue", self.sqs_client)
        
        url1 = queue.queue_url
        url2 = queue.queue_url
        
        assert url1 == url2

    def test_custom_visibility_timeout(self):
        queue = Queue("test-queue", self.sqs_client, visibility_timeout=3600)
        assert queue.visibility_timeout == 3600

    def test_worker_initialization(self):
        queue = Queue("test-queue", self.sqs_client)
        worker = Worker([queue], self.sqs_client, num_processes=4, grace_period=60, result_backend=self.result_backend)
        
        assert len(worker.queues) == 1
        assert worker.num_processes == 4
        assert worker.grace_period == 60
        assert worker.result_backend == self.result_backend

    def test_multiple_queues_worker(self):
        queue1 = Queue("queue1", self.sqs_client)
        queue2 = Queue("queue2", self.sqs_client)
        worker = Worker([queue1, queue2], self.sqs_client)
        
        assert len(worker.queues) == 2

    @patch("sqs_jobs.worker.ProcessPoolExecutor")
    @patch("time.sleep")
    def test_worker_graceful_shutdown(self, mock_sleep, mock_executor_class):
        mock_executor = Mock()
        mock_executor_class.return_value.__enter__.return_value = mock_executor
        
        queue = Queue("test-queue", self.sqs_client)
        worker = Worker([queue], self.sqs_client)
        
        worker._should_stop = True
        worker.work()
        
        mock_executor_class.assert_called_once()

    def test_result_backend_integration(self):
        queue = Queue("test-queue", self.sqs_client)
        
        job = queue.enqueue(add_numbers, 7, 3)
        
        result = 10
        self.result_backend.store(job.job_id, result)
        retrieved_result = self.result_backend.get(job.job_id)
        
        assert retrieved_result == result

    def test_job_result_storage(self):
        from sqs_jobs.job import Job
        
        job = Job.create(add_numbers, [5, 5])
        result = job.execute()
        
        assert result == 10
        
        # Result storage is now handled by the worker
        self.result_backend.store(job.job_id, result)
        assert self.result_backend.get(job.job_id) == 10

    def test_error_handling_in_job_execution(self):
        from sqs_jobs.job import Job
        
        job = Job.create(divide_numbers, [10, 0])
        
        with pytest.raises(ValueError, match="Division by zero"):
            job.execute()

    def test_message_attributes_in_queue(self):
        queue = Queue("test-queue", self.sqs_client)
        
        with patch.object(queue.sqs_client, "send_message") as mock_send:
            mock_send.return_value = {"MessageId": "msg-123"}
            
            job = queue.enqueue(add_numbers, 1, 2)
            
            mock_send.assert_called_once()
            call_kwargs = mock_send.call_args[1]
            
            assert "MessageAttributes" in call_kwargs
            attrs = call_kwargs["MessageAttributes"]
            assert "job_id" in attrs
            assert "function_name" in attrs
            assert "enqueue_timestamp" in attrs
            assert "queue_name" in attrs
            
            assert attrs["job_id"]["StringValue"] == job.job_id
            assert attrs["function_name"]["StringValue"] == job.function
            assert attrs["queue_name"]["StringValue"] == "test-queue"

    def test_job_serialization_roundtrip(self):
        from sqs_jobs.job import Job
        from datetime import datetime
        from decimal import Decimal
        
        job = Job.create(add_numbers, [1, 2], {"extra": Decimal("3.14")})
        
        json_data = job.to_json()
        reconstructed = Job.from_json(json_data)
        
        assert reconstructed.job_id == job.job_id
        assert reconstructed.function == job.function
        assert reconstructed.args == job.args
        assert reconstructed.kwargs == job.kwargs
        assert isinstance(reconstructed.enqueued_at, datetime)

    def test_queue_message_receive_and_delete(self):
        queue = Queue("test-queue", self.sqs_client)
        
        messages = queue.receive_jobs(max_messages=1, wait_time=1)
        assert isinstance(messages, list)
        
        if messages:
            queue.delete_message(messages[0]["ReceiptHandle"])

    def test_worker_repr(self):
        queue1 = Queue("queue1", self.sqs_client)
        queue2 = Queue("queue2", self.sqs_client)
        worker = Worker([queue1, queue2], self.sqs_client, num_processes=2)
        
        repr_str = repr(worker)
        assert "Worker" in repr_str
        assert "queue1" in repr_str
        assert "queue2" in repr_str
        assert "processes=2" in repr_str

    def test_queue_repr(self):
        queue = Queue("test-queue", self.sqs_client, queue_type="fifo")
        
        repr_str = repr(queue)
        assert "Queue" in repr_str
        assert "test-queue" in repr_str
        assert "fifo" in repr_str