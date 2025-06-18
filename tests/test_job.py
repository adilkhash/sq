import pytest
from datetime import datetime
from sqs_jobs.job import Job, job


def sample_function(x, y, z=10):
    return x + y + z


@job(timeout=300)
def decorated_function(a, b):
    return a * b


class TestJob:
    def test_job_creation(self):
        j = Job.create(sample_function, [1, 2], {"z": 3})
        assert j.job_id is not None
        assert j.function == "tests.test_job.sample_function"
        assert j.args == [1, 2]
        assert j.kwargs == {"z": 3}
        assert isinstance(j.enqueued_at, datetime)

    def test_job_to_dict(self):
        j = Job.create(sample_function, [1, 2], {"z": 3})
        data = j.to_dict()

        expected_keys = {"job_id", "function", "args", "kwargs", "enqueued_at"}
        assert set(data.keys()) == expected_keys
        assert data["function"] == "tests.test_job.sample_function"
        assert data["args"] == [1, 2]
        assert data["kwargs"] == {"z": 3}

    def test_job_from_dict(self):
        original = Job.create(sample_function, [1, 2], {"z": 3})
        data = original.to_dict()
        reconstructed = Job.from_dict(data)

        assert reconstructed.job_id == original.job_id
        assert reconstructed.function == original.function
        assert reconstructed.args == original.args
        assert reconstructed.kwargs == original.kwargs
        assert reconstructed.enqueued_at == original.enqueued_at

    def test_job_json_serialization(self):
        original = Job.create(sample_function, [1, 2], {"z": 3})
        json_data = original.to_json()
        reconstructed = Job.from_json(json_data)

        assert reconstructed.job_id == original.job_id
        assert reconstructed.function == original.function
        assert reconstructed.args == original.args
        assert reconstructed.kwargs == original.kwargs
        assert reconstructed.enqueued_at == original.enqueued_at

    def test_job_get_function(self):
        j = Job.create(sample_function, [1, 2])
        func = j.get_function()
        assert func == sample_function

    def test_job_execute(self):
        j = Job.create(sample_function, [1, 2], {"z": 3})
        result = j.execute()
        assert result == 6  # 1 + 2 + 3

    def test_job_execute_with_invalid_function(self):
        j = Job(function="nonexistent.module.function")
        with pytest.raises(ImportError):
            j.execute()

    def test_job_decorator(self):
        assert hasattr(decorated_function, "_sqs_job_timeout")
        assert decorated_function._sqs_job_timeout == 300

        result = decorated_function(3, 4)
        assert result == 12

    def test_job_decorator_invalid_target(self):
        with pytest.raises(TypeError):

            @job()
            class SomeClass:
                pass
