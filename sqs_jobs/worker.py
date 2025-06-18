import os
import signal
import time
import logging
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor, TimeoutError, as_completed
from typing import List, Optional, Any, Dict
from .queue import Queue
from .job import Job


logger = logging.getLogger(__name__)


def _execute_job_with_timeout(job_data: Dict[str, Any], timeout: Optional[int] = None) -> Any:
    try:
        job = Job.from_dict(job_data)
        
        if timeout:
            def timeout_handler(signum, frame):
                raise TimeoutError(f"Job {job.job_id} timed out after {timeout} seconds")
            
            signal.signal(signal.SIGALRM, timeout_handler)
            signal.alarm(timeout)
        
        try:
            result = job.execute()
            return {"success": True, "result": result, "job_id": job.job_id}
        finally:
            if timeout:
                signal.alarm(0)
                
    except Exception as e:
        logger.exception(f"Job {job_data.get('job_id', 'unknown')} failed: {e}")
        return {
            "success": False,
            "error": str(e),
            "job_id": job_data.get("job_id", "unknown"),
        }


class Worker:
    def __init__(
        self,
        queues: List[Queue],
        sqs_client: Any,
        num_processes: Optional[int] = None,
        grace_period: int = 30,
    ):
        self.queues = queues
        self.sqs_client = sqs_client
        self.num_processes = num_processes or mp.cpu_count()
        self.grace_period = grace_period
        self._should_stop = False
        self._executor = None
        self._active_jobs = {}

    def work(self) -> None:
        self._setup_signal_handlers()
        logger.info(f"Worker started with {self.num_processes} processes")
        
        with ProcessPoolExecutor(max_workers=self.num_processes) as executor:
            self._executor = executor
            
            while not self._should_stop:
                try:
                    self._process_queues()
                    time.sleep(1)
                except KeyboardInterrupt:
                    logger.info("Received interrupt signal, shutting down...")
                    break
                except Exception as e:
                    logger.exception(f"Error in worker loop: {e}")
                    time.sleep(5)
        
        logger.info("Worker stopped")

    def _setup_signal_handlers(self) -> None:
        def signal_handler(signum, frame):
            logger.info(f"Received signal {signum}, initiating graceful shutdown...")
            self._should_stop = True

        signal.signal(signal.SIGTERM, signal_handler)
        signal.signal(signal.SIGINT, signal_handler)

    def _process_queues(self) -> None:
        for queue in self.queues:
            if self._should_stop:
                break
            
            self._process_queue(queue)
            self._check_completed_jobs()

    def _process_queue(self, queue: Queue) -> None:
        messages = queue.receive_jobs(max_messages=1, wait_time=1)
        
        for message in messages:
            if self._should_stop:
                break
            
            try:
                job_data = Job.from_json(message["Body"]).to_dict()
                receipt_handle = message["ReceiptHandle"]
                timeout = job_data.get("timeout")
                
                future = self._executor.submit(_execute_job_with_timeout, job_data, timeout)
                self._active_jobs[future] = {
                    "queue": queue,
                    "receipt_handle": receipt_handle,
                    "job_id": job_data["job_id"],
                    "started_at": time.time(),
                }
                
                logger.info(f"Started job {job_data['job_id']} from queue {queue.name}")
                
            except Exception as e:
                logger.exception(f"Error processing message: {e}")
                queue.delete_message(message["ReceiptHandle"])

    def _check_completed_jobs(self) -> None:
        completed_futures = []
        
        for future in as_completed(self._active_jobs.keys(), timeout=0.1):
            completed_futures.append(future)
        
        for future in completed_futures:
            job_info = self._active_jobs.pop(future)
            queue = job_info["queue"]
            receipt_handle = job_info["receipt_handle"]
            job_id = job_info["job_id"]
            
            try:
                result = future.result()
                if result["success"]:
                    logger.info(f"Job {job_id} completed successfully")
                    queue.delete_message(receipt_handle)
                else:
                    logger.error(f"Job {job_id} failed: {result['error']}")
                    
            except TimeoutError:
                logger.error(f"Job {job_id} timed out")
            except Exception as e:
                logger.exception(f"Unexpected error in job {job_id}: {e}")

    def shutdown(self) -> None:
        logger.info("Initiating worker shutdown...")
        self._should_stop = True
        
        if self._executor and self._active_jobs:
            logger.info(f"Waiting for {len(self._active_jobs)} active jobs to complete...")
            
            start_time = time.time()
            while self._active_jobs and (time.time() - start_time) < self.grace_period:
                self._check_completed_jobs()
                time.sleep(0.5)
            
            if self._active_jobs:
                logger.warning(f"Forcibly terminating {len(self._active_jobs)} remaining jobs")
                for future in self._active_jobs:
                    future.cancel()

    def __repr__(self) -> str:
        queue_names = [q.name for q in self.queues]
        return f"Worker(queues={queue_names}, processes={self.num_processes})"