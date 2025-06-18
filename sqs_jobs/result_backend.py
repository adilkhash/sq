from abc import ABC, abstractmethod
from typing import Any, Dict, Optional
import time
from .serializer import serialize, deserialize


class ResultBackend(ABC):
    @abstractmethod
    def store(self, job_id: str, result: Any) -> None:
        pass

    @abstractmethod
    def get(self, job_id: str) -> Any:
        pass


class MemoryResultBackend(ResultBackend):
    def __init__(self, ttl: Optional[int] = None):
        self.ttl = ttl
        self._store: Dict[str, Dict[str, Any]] = {}

    def store(self, job_id: str, result: Any) -> None:
        data = {
            "result": result,
            "stored_at": time.time() if self.ttl else None,
        }
        self._store[job_id] = data

    def get(self, job_id: str) -> Any:
        if job_id not in self._store:
            raise KeyError(f"Result for job {job_id} not found")

        data = self._store[job_id]

        if self.ttl and data["stored_at"]:
            if time.time() - data["stored_at"] > self.ttl:
                del self._store[job_id]
                raise KeyError(f"Result for job {job_id} has expired")

        return data["result"]

    def cleanup_expired(self) -> int:
        if not self.ttl:
            return 0

        current_time = time.time()
        expired_keys = []

        for job_id, data in self._store.items():
            if data["stored_at"] and current_time - data["stored_at"] > self.ttl:
                expired_keys.append(job_id)

        for key in expired_keys:
            del self._store[key]

        return len(expired_keys)


class S3ResultBackend(ResultBackend):
    def __init__(
        self, s3_client: Any, bucket_name: str, key_prefix: str = "job-results/"
    ):
        self.s3_client = s3_client
        self.bucket_name = bucket_name
        self.key_prefix = key_prefix

    def _get_key(self, job_id: str) -> str:
        return f"{self.key_prefix}{job_id}.json"

    def store(self, job_id: str, result: Any) -> None:
        serialized_result = serialize(result)
        key = self._get_key(job_id)

        self.s3_client.put_object(
            Bucket=self.bucket_name,
            Key=key,
            Body=serialized_result,
            ContentType="application/json",
        )

    def get(self, job_id: str) -> Any:
        key = self._get_key(job_id)

        try:
            response = self.s3_client.get_object(Bucket=self.bucket_name, Key=key)
            content = response["Body"].read().decode("utf-8")
            return deserialize(content)
        except self.s3_client.exceptions.NoSuchKey:
            raise KeyError(f"Result for job {job_id} not found")


class RedisResultBackend(ResultBackend):
    def __init__(
        self, redis_client: Any, key_prefix: str = "sqs_job:", ttl: Optional[int] = None
    ):
        self.redis_client = redis_client
        self.key_prefix = key_prefix
        self.ttl = ttl

    def _get_key(self, job_id: str) -> str:
        return f"{self.key_prefix}{job_id}"

    def store(self, job_id: str, result: Any) -> None:
        serialized_result = serialize(result)
        key = self._get_key(job_id)

        if self.ttl:
            self.redis_client.setex(key, self.ttl, serialized_result)
        else:
            self.redis_client.set(key, serialized_result)

    def get(self, job_id: str) -> Any:
        key = self._get_key(job_id)
        content = self.redis_client.get(key)

        if content is None:
            raise KeyError(f"Result for job {job_id} not found")

        if isinstance(content, bytes):
            content = content.decode("utf-8")

        return deserialize(content)
