import time
from typing import Any, Dict, List, Optional, Callable
from botocore.exceptions import ClientError
from .job import Job


class Queue:
    def __init__(
        self,
        name: str,
        sqs_client: Any,
        queue_type: str = "standard",
        visibility_timeout: int = 1800,
        result_backend: Optional[Any] = None,
    ):
        self.name = name
        self.sqs_client = sqs_client
        self.queue_type = queue_type
        self.visibility_timeout = visibility_timeout
        self.result_backend = result_backend
        self._queue_url = None

    @property
    def queue_url(self) -> str:
        if self._queue_url is None:
            self._queue_url = self._get_or_create_queue()
        return self._queue_url

    def _get_or_create_queue(self) -> str:
        queue_name = self.name
        if self.queue_type == "fifo" and not queue_name.endswith(".fifo"):
            queue_name += ".fifo"

        try:
            response = self.sqs_client.get_queue_url(QueueName=queue_name)
            return response["QueueUrl"]
        except ClientError as e:
            if e.response["Error"]["Code"] == "AWS.SimpleQueueService.NonExistentQueue":
                return self._create_queue(queue_name)
            raise

    def _create_queue(self, queue_name: str) -> str:
        attributes = {
            "VisibilityTimeoutSeconds": str(self.visibility_timeout),
        }

        if self.queue_type == "fifo":
            attributes["FifoQueue"] = "true"
            attributes["ContentBasedDeduplication"] = "true"

        response = self.sqs_client.create_queue(
            QueueName=queue_name, Attributes=attributes
        )
        return response["QueueUrl"]

    def enqueue(
        self,
        func: Callable,
        *args,
        timeout: Optional[int] = None,
        result_backend: Optional[Any] = None,
        **kwargs
    ) -> Job:
        job_timeout = timeout
        job_result_backend = result_backend or self.result_backend

        if hasattr(func, "_sqs_job_timeout") and job_timeout is None:
            job_timeout = func._sqs_job_timeout
        if hasattr(func, "_sqs_job_result_backend") and job_result_backend is None:
            job_result_backend = func._sqs_job_result_backend

        job = Job.create(
            func=func,
            args=list(args),
            kwargs=kwargs,
            timeout=job_timeout,
            result_backend=job_result_backend,
        )

        self._send_job(job)
        return job

    def _send_job(self, job: Job) -> None:
        message_body = job.to_json()
        message_attributes = {
            "job_id": {"StringValue": job.job_id, "DataType": "String"},
            "function_name": {"StringValue": job.function, "DataType": "String"},
            "enqueue_timestamp": {
                "StringValue": str(int(job.enqueued_at.timestamp())),
                "DataType": "Number",
            },
            "queue_name": {"StringValue": self.name, "DataType": "String"},
        }

        send_params = {
            "QueueUrl": self.queue_url,
            "MessageBody": message_body,
            "MessageAttributes": message_attributes,
        }

        if self.queue_type == "fifo":
            send_params["MessageGroupId"] = "default"
            send_params["MessageDeduplicationId"] = job.job_id

        max_retries = 3
        retry_delay = 1

        for attempt in range(max_retries):
            try:
                self.sqs_client.send_message(**send_params)
                return
            except ClientError as e:
                if attempt == max_retries - 1:
                    raise
                time.sleep(retry_delay * (2 ** attempt))

    def receive_jobs(self, max_messages: int = 1, wait_time: int = 20) -> List[Dict[str, Any]]:
        try:
            response = self.sqs_client.receive_message(
                QueueUrl=self.queue_url,
                MaxNumberOfMessages=max_messages,
                WaitTimeSeconds=wait_time,
                MessageAttributeNames=["All"],
            )
            return response.get("Messages", [])
        except ClientError:
            return []

    def delete_message(self, receipt_handle: str) -> None:
        try:
            self.sqs_client.delete_message(
                QueueUrl=self.queue_url, ReceiptHandle=receipt_handle
            )
        except ClientError:
            pass

    def __repr__(self) -> str:
        return f"Queue(name={self.name}, type={self.queue_type})"
