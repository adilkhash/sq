import pytest
import time
from unittest.mock import Mock
from sqs_jobs.result_backend import MemoryResultBackend, S3ResultBackend, RedisResultBackend


class TestMemoryResultBackend:
    def test_store_and_get_result(self):
        backend = MemoryResultBackend()
        result = {"status": "completed", "data": [1, 2, 3]}
        
        backend.store("job-123", result)
        retrieved = backend.get("job-123")
        
        assert retrieved == result

    def test_get_nonexistent_result(self):
        backend = MemoryResultBackend()
        
        with pytest.raises(KeyError, match="Result for job nonexistent not found"):
            backend.get("nonexistent")

    def test_ttl_functionality(self):
        backend = MemoryResultBackend(ttl=1)
        result = "test result"
        
        backend.store("job-123", result)
        assert backend.get("job-123") == result
        
        time.sleep(1.1)
        
        with pytest.raises(KeyError, match="Result for job job-123 has expired"):
            backend.get("job-123")

    def test_cleanup_expired(self):
        backend = MemoryResultBackend(ttl=1)
        
        backend.store("job-1", "result1")
        backend.store("job-2", "result2")
        
        time.sleep(1.1)
        
        expired_count = backend.cleanup_expired()
        assert expired_count == 2
        
        with pytest.raises(KeyError):
            backend.get("job-1")
        with pytest.raises(KeyError):
            backend.get("job-2")

    def test_no_ttl_no_expiry(self):
        backend = MemoryResultBackend()
        result = "persistent result"
        
        backend.store("job-123", result)
        time.sleep(0.1)
        
        assert backend.get("job-123") == result
        assert backend.cleanup_expired() == 0


class TestS3ResultBackend:
    def setup_method(self):
        self.mock_s3_client = Mock()
        self.backend = S3ResultBackend(self.mock_s3_client, "test-bucket")

    def test_store_result(self):
        result = {"status": "completed", "value": 42}
        
        self.backend.store("job-123", result)
        
        self.mock_s3_client.put_object.assert_called_once()
        call_args = self.mock_s3_client.put_object.call_args[1]
        assert call_args["Bucket"] == "test-bucket"
        assert call_args["Key"] == "job-results/job-123.json"
        assert call_args["ContentType"] == "application/json"

    def test_get_result(self):
        mock_response = {
            "Body": Mock()
        }
        mock_response["Body"].read.return_value = b'{"status":"completed","value":42}'
        self.mock_s3_client.get_object.return_value = mock_response
        
        result = self.backend.get("job-123")
        
        assert result == {"status": "completed", "value": 42}
        self.mock_s3_client.get_object.assert_called_once_with(
            Bucket="test-bucket",
            Key="job-results/job-123.json"
        )

    def test_get_nonexistent_result(self):
        self.mock_s3_client.exceptions.NoSuchKey = Exception
        self.mock_s3_client.get_object.side_effect = self.mock_s3_client.exceptions.NoSuchKey()
        
        with pytest.raises(KeyError, match="Result for job job-123 not found"):
            self.backend.get("job-123")

    def test_custom_key_prefix(self):
        backend = S3ResultBackend(self.mock_s3_client, "test-bucket", "custom/prefix/")
        
        backend.store("job-123", "result")
        
        call_args = self.mock_s3_client.put_object.call_args[1]
        assert call_args["Key"] == "custom/prefix/job-123.json"


class TestRedisResultBackend:
    def setup_method(self):
        self.mock_redis_client = Mock()
        self.backend = RedisResultBackend(self.mock_redis_client)

    def test_store_result_without_ttl(self):
        result = {"status": "completed", "value": 42}
        
        self.backend.store("job-123", result)
        
        self.mock_redis_client.set.assert_called_once()
        call_args = self.mock_redis_client.set.call_args[0]
        assert call_args[0] == "sqs_job:job-123"

    def test_store_result_with_ttl(self):
        backend = RedisResultBackend(self.mock_redis_client, ttl=3600)
        result = {"status": "completed", "value": 42}
        
        backend.store("job-123", result)
        
        self.mock_redis_client.setex.assert_called_once()
        call_args = self.mock_redis_client.setex.call_args[0]
        assert call_args[0] == "sqs_job:job-123"
        assert call_args[1] == 3600

    def test_get_result_string(self):
        self.mock_redis_client.get.return_value = '{"status":"completed","value":42}'
        
        result = self.backend.get("job-123")
        
        assert result == {"status": "completed", "value": 42}
        self.mock_redis_client.get.assert_called_once_with("sqs_job:job-123")

    def test_get_result_bytes(self):
        self.mock_redis_client.get.return_value = b'{"status":"completed","value":42}'
        
        result = self.backend.get("job-123")
        
        assert result == {"status": "completed", "value": 42}

    def test_get_nonexistent_result(self):
        self.mock_redis_client.get.return_value = None
        
        with pytest.raises(KeyError, match="Result for job job-123 not found"):
            self.backend.get("job-123")

    def test_custom_key_prefix(self):
        backend = RedisResultBackend(self.mock_redis_client, key_prefix="custom:")
        
        backend.store("job-123", "result")
        
        call_args = self.mock_redis_client.set.call_args[0]
        assert call_args[0] == "custom:job-123"