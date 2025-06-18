import pytest
import boto3
from moto import mock_aws
from sqs_jobs.result_backend import MemoryResultBackend


@pytest.fixture
def sqs_client():
    with mock_aws():
        yield boto3.client("sqs", region_name="us-east-1")


@pytest.fixture
def memory_result_backend():
    return MemoryResultBackend(ttl=3600)