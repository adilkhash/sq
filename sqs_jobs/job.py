import uuid
import importlib
import inspect
from datetime import datetime
from typing import Any, Dict, List, Optional, Callable
from .serializer import serialize, deserialize


class Job:
    def __init__(
        self,
        job_id: Optional[str] = None,
        function: Optional[str] = None,
        args: Optional[List[Any]] = None,
        kwargs: Optional[Dict[str, Any]] = None,
        enqueued_at: Optional[datetime] = None,
        timeout: Optional[int] = None,
    ):
        self.job_id = job_id or str(uuid.uuid4())
        self.function = function
        self.args = args or []
        self.kwargs = kwargs or {}
        self.enqueued_at = enqueued_at or datetime.utcnow()
        self.timeout = timeout

    @classmethod
    def create(
        cls,
        func: Callable,
        args: Optional[List[Any]] = None,
        kwargs: Optional[Dict[str, Any]] = None,
        timeout: Optional[int] = None,
    ) -> "Job":
        function_name = f"{func.__module__}.{func.__name__}"
        return cls(
            function=function_name,
            args=args or [],
            kwargs=kwargs or {},
            timeout=timeout,
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "job_id": self.job_id,
            "function": self.function,
            "args": self.args,
            "kwargs": self.kwargs,
            "enqueued_at": self.enqueued_at,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Job":
        return cls(
            job_id=data["job_id"],
            function=data["function"],
            args=data["args"],
            kwargs=data["kwargs"],
            enqueued_at=data["enqueued_at"],
        )

    def to_json(self) -> str:
        return serialize(self.to_dict())

    @classmethod
    def from_json(cls, json_data: str) -> "Job":
        data = deserialize(json_data)
        return cls.from_dict(data)

    def get_function(self) -> Callable:
        if not self.function:
            raise ValueError("No function specified for job")
        
        module_name, function_name = self.function.rsplit(".", 1)
        try:
            module = importlib.import_module(module_name)
            return getattr(module, function_name)
        except (ImportError, AttributeError) as e:
            raise ImportError(f"Could not import function {self.function}: {e}")

    def execute(self) -> Any:
        func = self.get_function()
        return func(*self.args, **self.kwargs)

    def __repr__(self) -> str:
        return f"Job(id={self.job_id}, function={self.function})"


def job(timeout: Optional[int] = None):
    def decorator(func: Callable) -> Callable:
        if not inspect.isfunction(func):
            raise TypeError("@job decorator can only be applied to functions")
        
        func._sqs_job_timeout = timeout
        return func
    
    return decorator