import logging
from typing import Tuple, Any, Dict, Optional, Callable

from rearq.exceptions import SerializationError, DeserializationError
from rearq.jobs import JobDef, JobResult
from rearq.utils import ms_to_datetime

Serializer = Callable[[Dict[str, Any]], bytes]
Deserializer = Callable[[bytes], Dict[str, Any]]

logger = logging.getLogger("serialize")


def serialize_job(
        function: str,
        args: Tuple[Any, ...],
        kwargs: Dict[str, Any],
        retry_times: Optional[int],
        enqueue_ms: int,
        serializer: Optional[Serializer] = None,
) -> Optional[bytes]:
    data = {"retry_times": retry_times, "function": function, "args": args, "kwargs": kwargs, "enqueue_ms": enqueue_ms}
    try:
        return serializer(data)
    except Exception as e:
        raise SerializationError(f'unable to serialize job "{function}"') from e


def serialize_result(
        function: str,
        args: Tuple[Any, ...],
        kwargs: Dict[str, Any],
        job_try: int,
        enqueue_time_ms: int,
        success: bool,
        result: Any,
        start_ms: int,
        finished_ms: int,
        ref: str,
        *,
        serializer: Optional[Serializer] = None,
) -> Optional[bytes]:
    data = {
        "t": job_try,
        "f": function,
        "a": args,
        "k": kwargs,
        "et": enqueue_time_ms,
        "s": success,
        "r": result,
        "st": start_ms,
        "ft": finished_ms,
    }

    try:
        return serializer(data)
    except Exception:
        logger.warning("error serializing result of %s", ref, exc_info=True)

    # use string in case serialization fails again
    data.update(r="unable to serialize result", s=False)
    try:
        return serializer(data)
    except Exception:
        logger.critical(
            "error serializing result of %s even after replacing result", ref, exc_info=True
        )
    return None


def deserialize_job(r: bytes, *, deserializer: Optional[Deserializer] = None) -> JobDef:
    try:
        d = deserializer(r)
        return JobDef(
            function=d["f"],
            args=d["a"],
            kwargs=d["k"],
            job_try=d["t"],
            enqueue_time=ms_to_datetime(d["et"]),
            score=None,
        )
    except Exception as e:
        raise DeserializationError("unable to deserialize job") from e


def deserialize_job_raw(
        r: bytes, *, deserializer: Optional[Deserializer] = None
) -> Tuple[str, Tuple[Any, ...], Dict[str, Any], int, int]:
    try:
        d = deserializer(r)
        return d["f"], d["a"], d["k"], d["t"], d["et"]
    except Exception as e:
        raise DeserializationError("unable to deserialize job") from e


def deserialize_result(r: bytes, *, deserializer: Optional[Deserializer] = None) -> JobResult:
    try:
        d = deserializer(r)
        return JobResult(
            job_try=d["t"],
            function=d["f"],
            args=d["a"],
            kwargs=d["k"],
            enqueue_time=ms_to_datetime(d["et"]),
            score=None,
            success=d["s"],
            result=d["r"],
            start_time=ms_to_datetime(d["st"]),
            finish_time=ms_to_datetime(d["ft"]),
        )
    except Exception as e:
        raise DeserializationError("unable to deserialize job result") from e
