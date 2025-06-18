import json
import base64
from datetime import datetime, date, time
from decimal import Decimal
from uuid import UUID
from typing import Any


class SQSJobsEncoder(json.JSONEncoder):
    def default(self, obj: Any) -> Any:
        if isinstance(obj, datetime):
            return {"__type__": "datetime", "value": obj.isoformat()}
        elif isinstance(obj, date):
            return {"__type__": "date", "value": obj.isoformat()}
        elif isinstance(obj, time):
            return {"__type__": "time", "value": obj.isoformat()}
        elif isinstance(obj, Decimal):
            return {"__type__": "decimal", "value": str(obj)}
        elif isinstance(obj, UUID):
            return {"__type__": "uuid", "value": str(obj)}
        elif isinstance(obj, bytes):
            return {"__type__": "bytes", "value": base64.b64encode(obj).decode("ascii")}
        elif isinstance(obj, set):
            return {"__type__": "set", "value": list(obj)}
        elif isinstance(obj, tuple):
            return {"__type__": "tuple", "value": list(obj)}
        return super().default(obj)


def _decode_object(obj: dict) -> Any:
    if "__type__" not in obj:
        return obj
    
    obj_type = obj["__type__"]
    value = obj["value"]
    
    if obj_type == "datetime":
        return datetime.fromisoformat(value)
    elif obj_type == "date":
        return date.fromisoformat(value)
    elif obj_type == "time":
        return time.fromisoformat(value)
    elif obj_type == "decimal":
        return Decimal(value)
    elif obj_type == "uuid":
        return UUID(value)
    elif obj_type == "bytes":
        return base64.b64decode(value.encode("ascii"))
    elif obj_type == "set":
        return set(value)
    elif obj_type == "tuple":
        return tuple(value)
    
    return obj


def serialize(data: Any) -> str:
    return json.dumps(data, cls=SQSJobsEncoder, separators=(",", ":"))


def deserialize(data: str) -> Any:
    return json.loads(data, object_hook=_decode_object)