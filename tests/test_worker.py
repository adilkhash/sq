import pytest
import time
from unittest.mock import Mock, patch, MagicMock
from sqs_jobs.worker import Worker, _execute_job_with_timeout
from sqs_jobs.queue import Queue
from sqs_jobs.job import Job


def sample_task(x, y):
    return x + y


def slow_task():
    time.sleep(2)
    return "completed"


def failing_task():
    raise ValueError("Task failed")


class TestWorkerJobExecutor:
    def test_execute_job_with_timeout_success(self):
        job = Job.create(sample_task, [1, 2])
        job_data = job.to_dict()
        
        result = _execute_job_with_timeout(job_data)
        
        assert result["success"] is True
        assert result["result"] == 3
        assert result["job_id"] == job.job_id

    def test_execute_job_with_result_backend(self):
        from sqs_jobs.result_backend import MemoryResultBackend
        
        result_backend = MemoryResultBackend()
        job = Job.create(sample_task, [1, 2])
        job_data = job.to_dict()
        
        result = _execute_job_with_timeout(job_data, result_backend=result_backend)
        
        assert result["success"] is True
        assert result["result"] == 3
        assert result_backend.get(job.job_id) == 3

    def test_execute_job_with_timeout_failure(self):
        job = Job.create(failing_task)
        job_data = job.to_dict()
        
        result = _execute_job_with_timeout(job_data)
        
        assert result["success"] is False
        assert "Task failed" in result["error"]
        assert result["job_id"] == job.job_id

    @patch("signal.alarm")
    @patch("signal.signal")
    def test_execute_job_with_timeout_setting(self, mock_signal, mock_alarm):
        job = Job.create(sample_task, [1, 2])
        job_data = job.to_dict()
        
        _execute_job_with_timeout(job_data, timeout=30)
        
        mock_signal.assert_called()
        mock_alarm.assert_any_call(30)
        mock_alarm.assert_any_call(0)


class TestWorker:
    def setup_method(self):
        self.mock_sqs_client = Mock()
        self.mock_queue = Mock(spec=Queue)
        self.mock_queue.name = "test-queue"
        self.mock_queue.receive_jobs.return_value = []
        
        self.worker = Worker([self.mock_queue], self.mock_sqs_client, num_processes=2)

    def test_worker_initialization(self):
        assert self.worker.queues == [self.mock_queue]
        assert self.worker.sqs_client == self.mock_sqs_client
        assert self.worker.num_processes == 2
        assert self.worker.grace_period == 30
        assert self.worker.result_backend is None
        assert self.worker._should_stop is False

    def test_worker_default_num_processes(self):
        worker = Worker([self.mock_queue], self.mock_sqs_client)
        assert worker.num_processes > 0

    def test_worker_with_result_backend(self):
        from sqs_jobs.result_backend import MemoryResultBackend
        
        result_backend = MemoryResultBackend()
        worker = Worker([self.mock_queue], self.mock_sqs_client, result_backend=result_backend)
        assert worker.result_backend == result_backend

    @patch("sqs_jobs.worker.ProcessPoolExecutor")
    @patch("sqs_jobs.worker.time.sleep")
    def test_worker_work_loop(self, mock_sleep, mock_executor_class):
        mock_executor = MagicMock()
        mock_executor_class.return_value.__enter__.return_value = mock_executor
        
        self.worker._should_stop = True
        
        self.worker.work()
        
        mock_executor_class.assert_called_once_with(max_workers=2)

    def test_process_queue_with_message(self):
        job = Job.create(sample_task, [1, 2])
        message = {
            "Body": job.to_json(),
            "ReceiptHandle": "receipt-123"
        }
        self.mock_queue.receive_jobs.return_value = [message]
        
        with patch.object(self.worker, "_executor") as mock_executor:
            mock_future = Mock()
            mock_executor.submit.return_value = mock_future
            
            self.worker._process_queue(self.mock_queue)
            
            mock_executor.submit.assert_called_once()
            assert mock_future in self.worker._active_jobs

    def test_process_queue_empty(self):
        self.mock_queue.receive_jobs.return_value = []
        
        with patch.object(self.worker, "_executor"):
            self.worker._process_queue(self.mock_queue)

    @patch("sqs_jobs.worker.as_completed")
    def test_check_completed_jobs_success(self, mock_as_completed):
        mock_future = Mock()
        mock_future.result.return_value = {
            "success": True,
            "result": "test_result",
            "job_id": "job-123"
        }
        mock_as_completed.return_value = [mock_future]
        
        self.worker._active_jobs[mock_future] = {
            "queue": self.mock_queue,
            "receipt_handle": "receipt-123",
            "job_id": "job-123",
            "started_at": time.time()
        }
        
        self.worker._check_completed_jobs()
        
        self.mock_queue.delete_message.assert_called_once_with("receipt-123")
        assert mock_future not in self.worker._active_jobs

    @patch("sqs_jobs.worker.as_completed")
    def test_check_completed_jobs_failure(self, mock_as_completed):
        mock_future = Mock()
        mock_future.result.return_value = {
            "success": False,
            "error": "Task failed",
            "job_id": "job-123"
        }
        mock_as_completed.return_value = [mock_future]
        
        self.worker._active_jobs[mock_future] = {
            "queue": self.mock_queue,
            "receipt_handle": "receipt-123",
            "job_id": "job-123",
            "started_at": time.time()
        }
        
        self.worker._check_completed_jobs()
        
        self.mock_queue.delete_message.assert_not_called()
        assert mock_future not in self.worker._active_jobs

    def test_shutdown(self):
        assert self.worker._should_stop is False
        
        self.worker.shutdown()
        
        assert self.worker._should_stop is True

    def test_shutdown_with_active_jobs(self):
        mock_future = Mock()
        self.worker._active_jobs[mock_future] = {
            "queue": self.mock_queue,
            "receipt_handle": "receipt-123",
            "job_id": "job-123",
            "started_at": time.time()
        }
        
        with patch("time.sleep"):
            self.worker.shutdown()
        
        assert self.worker._should_stop is True