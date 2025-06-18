import pytest
from unittest.mock import Mock, MagicMock
from botocore.exceptions import ClientError
from sqs_jobs.queue import Queue
from sqs_jobs.job import job


def sample_task(x, y):
    return x + y


@job(timeout=300)
def decorated_task(a, b):
    return a * b


class TestQueue:
    def setup_method(self):
        self.mock_sqs_client = Mock()
        self.mock_sqs_client.get_queue_url.return_value = {
            "QueueUrl": "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue"
        }
        self.queue = Queue("test-queue", self.mock_sqs_client)

    def test_queue_initialization(self):
        assert self.queue.name == "test-queue"
        assert self.queue.sqs_client == self.mock_sqs_client
        assert self.queue.queue_type == "standard"
        assert self.queue.visibility_timeout == 1800

    def test_get_existing_queue_url(self):
        url = self.queue.queue_url
        assert url == "https://sqs.us-east-1.amazonaws.com/123456789012/test-queue"
        self.mock_sqs_client.get_queue_url.assert_called_once_with(
            QueueName="test-queue"
        )

    def test_create_queue_when_not_exists(self):
        self.mock_sqs_client.get_queue_url.side_effect = ClientError(
            {"Error": {"Code": "AWS.SimpleQueueService.NonExistentQueue"}},
            "GetQueueUrl",
        )
        self.mock_sqs_client.create_queue.return_value = {
            "QueueUrl": "https://sqs.us-east-1.amazonaws.com/123456789012/new-queue"
        }

        url = self.queue.queue_url
        assert url == "https://sqs.us-east-1.amazonaws.com/123456789012/new-queue"
        self.mock_sqs_client.create_queue.assert_called_once()

    def test_create_fifo_queue(self):
        fifo_queue = Queue("test-fifo", self.mock_sqs_client, queue_type="fifo")
        self.mock_sqs_client.get_queue_url.side_effect = ClientError(
            {"Error": {"Code": "AWS.SimpleQueueService.NonExistentQueue"}},
            "GetQueueUrl",
        )
        self.mock_sqs_client.create_queue.return_value = {
            "QueueUrl": "https://sqs.us-east-1.amazonaws.com/123456789012/test-fifo.fifo"
        }

        url = fifo_queue.queue_url
        self.mock_sqs_client.create_queue.assert_called_once_with(
            QueueName="test-fifo.fifo",
            Attributes={
                "VisibilityTimeoutSeconds": "1800",
                "FifoQueue": "true",
                "ContentBasedDeduplication": "true",
            },
        )

    def test_enqueue_job(self):
        self.mock_sqs_client.send_message.return_value = {"MessageId": "msg-123"}

        job = self.queue.enqueue(sample_task, 1, 2)

        assert job.function == "tests.test_queue.sample_task"
        assert job.args == [1, 2]
        assert job.kwargs == {}

        self.mock_sqs_client.send_message.assert_called_once()
        call_args = self.mock_sqs_client.send_message.call_args[1]
        assert "QueueUrl" in call_args
        assert "MessageBody" in call_args
        assert "MessageAttributes" in call_args

    def test_enqueue_job_with_decorated_function(self):
        self.mock_sqs_client.send_message.return_value = {"MessageId": "msg-123"}

        job = self.queue.enqueue(decorated_task, 3, 4)

        assert job.timeout == 300
        self.mock_sqs_client.send_message.assert_called_once()

    def test_enqueue_fifo_job(self):
        fifo_queue = Queue("test-fifo", self.mock_sqs_client, queue_type="fifo")
        self.mock_sqs_client.send_message.return_value = {"MessageId": "msg-123"}

        job = fifo_queue.enqueue(sample_task, 1, 2)

        self.mock_sqs_client.send_message.assert_called_once()
        call_args = self.mock_sqs_client.send_message.call_args[1]
        assert "MessageGroupId" in call_args
        assert "MessageDeduplicationId" in call_args
        assert call_args["MessageGroupId"] == "default"
        assert call_args["MessageDeduplicationId"] == job.job_id

    def test_send_message_with_retry(self):
        self.mock_sqs_client.send_message.side_effect = [
            ClientError({"Error": {"Code": "ServiceUnavailable"}}, "SendMessage"),
            {"MessageId": "msg-123"},
        ]

        job = self.queue.enqueue(sample_task, 1, 2)
        assert self.mock_sqs_client.send_message.call_count == 2

    def test_receive_jobs(self):
        self.mock_sqs_client.receive_message.return_value = {
            "Messages": [
                {
                    "Body": '{"job_id": "123", "function": "test.func"}',
                    "ReceiptHandle": "receipt-123",
                    "MessageAttributes": {},
                }
            ]
        }

        messages = self.queue.receive_jobs()
        assert len(messages) == 1
        assert messages[0]["Body"] == '{"job_id": "123", "function": "test.func"}'

        self.mock_sqs_client.receive_message.assert_called_once_with(
            QueueUrl=self.queue.queue_url,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=20,
            MessageAttributeNames=["All"],
        )

    def test_receive_jobs_empty(self):
        self.mock_sqs_client.receive_message.return_value = {}

        messages = self.queue.receive_jobs()
        assert messages == []

    def test_delete_message(self):
        self.queue.delete_message("receipt-123")
        self.mock_sqs_client.delete_message.assert_called_once_with(
            QueueUrl=self.queue.queue_url, ReceiptHandle="receipt-123"
        )
