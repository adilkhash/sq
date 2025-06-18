import pytest
from datetime import datetime, date, time
from decimal import Decimal
from uuid import UUID, uuid4
from sqs_jobs.serializer import serialize, deserialize


class TestSerializer:
    def test_serialize_deserialize_basic_types(self):
        data = {
            "string": "hello",
            "int": 42,
            "float": 3.14,
            "bool": True,
            "none": None,
            "list": [1, 2, 3],
            "dict": {"nested": "value"},
        }
        serialized = serialize(data)
        deserialized = deserialize(serialized)
        assert deserialized == data

    def test_serialize_deserialize_datetime(self):
        dt = datetime(2024, 1, 1, 12, 30, 45, 123456)
        data = {"datetime": dt}
        serialized = serialize(data)
        deserialized = deserialize(serialized)
        assert deserialized["datetime"] == dt

    def test_serialize_deserialize_date(self):
        d = date(2024, 1, 1)
        data = {"date": d}
        serialized = serialize(data)
        deserialized = deserialize(serialized)
        assert deserialized["date"] == d

    def test_serialize_deserialize_time(self):
        t = time(12, 30, 45, 123456)
        data = {"time": t}
        serialized = serialize(data)
        deserialized = deserialize(serialized)
        assert deserialized["time"] == t

    def test_serialize_deserialize_decimal(self):
        decimal_val = Decimal("123.456")
        data = {"decimal": decimal_val}
        serialized = serialize(data)
        deserialized = deserialize(serialized)
        assert deserialized["decimal"] == decimal_val

    def test_serialize_deserialize_uuid(self):
        uuid_val = uuid4()
        data = {"uuid": uuid_val}
        serialized = serialize(data)
        deserialized = deserialize(serialized)
        assert deserialized["uuid"] == uuid_val

    def test_serialize_deserialize_bytes(self):
        bytes_val = b"hello world"
        data = {"bytes": bytes_val}
        serialized = serialize(data)
        deserialized = deserialize(serialized)
        assert deserialized["bytes"] == bytes_val

    def test_serialize_deserialize_set(self):
        set_val = {1, 2, 3}
        data = {"set": set_val}
        serialized = serialize(data)
        deserialized = deserialize(serialized)
        assert deserialized["set"] == set_val

    def test_serialize_deserialize_tuple(self):
        tuple_val = [1, 2, 3]
        data = {"tuple": tuple_val}
        serialized = serialize(data)
        deserialized = deserialize(serialized)
        assert deserialized["tuple"] == tuple_val

    def test_serialize_deserialize_complex_nested(self):
        data = {
            "user_id": uuid4(),
            "created_at": datetime.now(),
            "metadata": {
                "tags": {"important", "urgent"},
                "coordinates": [40.7128, -74.0060],
                "price": Decimal("99.99"),
                "binary_data": b"secret",
            },
        }
        serialized = serialize(data)
        deserialized = deserialize(serialized)
        assert deserialized == data
