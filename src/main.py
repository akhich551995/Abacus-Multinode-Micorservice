import hashlib
import json
import logging
import time
from typing import Any, Dict, List, Optional

from fastapi import Depends, FastAPI, Request, HTTPException
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, field_validator
import redis
from starlette.requests import Request as StarletteRequest
from redis.exceptions import BusyLoadingError, ClusterDownError, ConnectionError as RedisConnectionError, TimeoutError as RedisTimeoutError

from .metrics import HTTP_REQUEST_COUNTER, HTTP_REQUEST_LATENCY, create_metrics_app
from .storage import (
    MAX_REDIS_INT,
    MIN_REDIS_INT,
    RedisAtomicError,
    RedisOverflowError,
    RedisUnderflowError,
    SUM_KEY,
    get_sum as storage_get_sum,
    increment_sum,
    redis_delete,
    redis_get,
    redis_ping,
    redis_set,
    redis_setnx,
    set_sum as storage_set_sum,
)
from .security import SecurityContext, security_dependency

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()
app.mount("/metrics", create_metrics_app())

IDEMPOTENCY_KEY_HEADER = "Idempotency-Key"
IDEMPOTENCY_KEY_PREFIX = "abacus:idempotency:"
IDEMPOTENCY_TTL_SECONDS = 60 * 60 * 24  # 24 hours


RETRYABLE_REDIS_ERRORS = (RedisConnectionError, RedisTimeoutError, BusyLoadingError, ClusterDownError)


def _raise_redis_unavailable(message: str, err: BaseException) -> None:
    logger.error("Redis unavailable during %s: %s", message, err)
    raise HTTPException(
        status_code=503,
        detail="Service Unavailable: " + message,
        headers={"Retry-After": "1"},
    ) from err


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: StarletteRequest, exc: RequestValidationError):
    """Return a consistent 400 response for all validation errors."""
    content_type = (request.headers.get("content-type") or "").lower()
    errors: List[Dict[str, Any]] = []
    
    def _clean_message(msg: str) -> str:
        if isinstance(msg, str) and msg.lower().startswith("value error") and "," in msg:
            _, remainder = msg.split(",", 1)
            return remainder.strip()
        return msg

    for err in exc.errors():
        location = ".".join(str(part) for part in err.get("loc", ()) if part != "body") or "body"
        errors.append(
            {
                "field": location,
                "message": _clean_message(err.get("msg", "Invalid value.")),
                "type": err.get("type", "validation_error"),
            }
        )

    summary = "Invalid request payload."
    if content_type and "application/json" not in content_type:
        summary = "Content-Type must be application/json."
    elif any(
        err.get("field") == "body" and err.get("type") in {"value_error.missing", "missing"}
        for err in errors
    ):
        summary = "Request body is required."
    elif any(err.get("type") in {"json_invalid", "value_error.jsondecode"} for err in errors):
        summary = "Invalid JSON payload."
    elif errors:
        first_err = errors[0]
        if first_err.get("field") == "number":
            summary = first_err.get("message", summary)

    return JSONResponse(status_code=400, content={"message": summary, "errors": errors})


@app.middleware("http")
async def http_metrics_middleware(request: Request, call_next):
    if request.url.path == "/metrics":
        return await call_next(request)

    start_time = time.perf_counter()
    status_code = "500"
    try:
        response = await call_next(request)
        status_code = str(response.status_code)
        return response
    finally:
        duration = time.perf_counter() - start_time
        labels = {
            "endpoint": request.url.path,
            "method": request.method,
            "status": status_code,
        }
        HTTP_REQUEST_COUNTER.labels(**labels).inc()
        HTTP_REQUEST_LATENCY.labels(endpoint=request.url.path, method=request.method).observe(duration)

@app.middleware("http")
async def db_connection_middleware(request: Request, call_next):
    """
    Middleware to handle Redis connection errors globally.
    """
    try:
        redis_ping()
        response = await call_next(request)
        return response
    except RETRYABLE_REDIS_ERRORS as err:
        logger.error("Redis connectivity failure: %s", err)
        return JSONResponse(
            status_code=503,
            headers={"Retry-After": "1"},
            content={"message": "Service Unavailable: Could not connect to the database."},
        )

class NumberInput(BaseModel):
    number: int

    @field_validator("number", mode="before")
    @classmethod
    def validate_number(cls, value: Any) -> int:
        if value is None:
            raise ValueError("Number is required.")
        if isinstance(value, bool):
            raise ValueError("Value must be an integer, not boolean.")
        if isinstance(value, int):
            if value < MIN_REDIS_INT or value > MAX_REDIS_INT:
                raise ValueError("Value must be within signed 64-bit integer range.")
            return value
        raise ValueError("Value must be a 64-bit integer.")


def _normalize_idempotency_key(request: Request, payload: Dict[str, Any]) -> Dict[str, Any]:
    """Return normalized cache key metadata for idempotent operations."""
    raw_header = request.headers.get(IDEMPOTENCY_KEY_HEADER)
    header_value = raw_header.strip() if isinstance(raw_header, str) else ""

    if header_value:
        key_source = header_value
        header_digest = hashlib.sha256(header_value.encode("utf-8")).hexdigest()
    else:
        key_source = json.dumps(
            {
                "method": request.method,
                "path": request.url.path,
                "body": payload,
            },
            sort_keys=True,
        )
        header_digest = None

    cache_digest = hashlib.sha256(key_source.encode("utf-8")).hexdigest()
    cache_key = f"{IDEMPOTENCY_KEY_PREFIX}{cache_digest}"
    return {
        "cache_key": cache_key,
        "header_value": header_value or None,
        "header_digest": header_digest,
    }

@app.on_event("startup")
async def startup_event():
    """
    On startup, try to connect to Redis and initialize the sum if not present.
    This helps in failing fast if the database is not available.
    """
    try:
        redis_ping()
        redis_setnx(SUM_KEY, 0)
        logger.info("Successfully connected to Redis and initialized sum.")
    except RETRYABLE_REDIS_ERRORS as e:
        logger.error(f"Could not connect to Redis on startup: {e}")
        # The app will start, but middleware will return 503.

@app.post("/abacus/number")
async def add_number(
    request: Request,
    data: NumberInput,
    _security: SecurityContext = Depends(security_dependency),
):
    """Adds the provided number to the running sum stored in Redis."""
    payload_dict = data.model_dump()
    # Idempotency is optional. If the client supplies an Idempotency-Key header
    # we will attempt to ensure the request is processed only once and return
    # the same response for retries. If no header is provided we still compute
    # a deterministic cache key based on the request payload so retries with the
    # same body are idempotent.
    raw_header = request.headers.get(IDEMPOTENCY_KEY_HEADER)
    header_value = raw_header.strip() if isinstance(raw_header, str) else ""
    if header_value and len(header_value) > 200:
        raise HTTPException(status_code=400, detail="Idempotency key is too long.")

    key_info = _normalize_idempotency_key(request, payload_dict)
    cache_key = key_info["cache_key"]
    header_digest = key_info["header_digest"]

    # Fast-path: if a cached entry already exists return it
    cached_payload_raw = redis_get(cache_key)
    if isinstance(cached_payload_raw, (bytes, str)) and cached_payload_raw:
        try:
            cached_payload = (
                cached_payload_raw.decode("utf-8") if isinstance(cached_payload_raw, bytes) else cached_payload_raw
            )
            cached_entry = json.loads(cached_payload)
        except json.JSONDecodeError:
            logger.warning("Corrupted idempotency cache entry for key %s; purging.", cache_key)
            redis_delete(cache_key)
        else:
            cached_request = cached_entry.get("request", {})
            cached_response = cached_entry.get("response")
            cached_header_digest = cached_entry.get("header_digest")
            if header_value and cached_request != payload_dict:
                raise HTTPException(
                    status_code=409,
                    detail="Idempotency key conflict: request payload differs from original request.",
                )
            if header_value and cached_header_digest and header_digest != cached_header_digest:
                raise HTTPException(
                    status_code=409,
                    detail="Idempotency key conflict: request payload differs from original request.",
                )
            if cached_response is not None:
                return cached_response

    # Reservation key prevents multiple processes from performing the same
    # operation concurrently. We use SET NX with an expiry.
    reservation_key = f"{cache_key}:lock"
    try:
        reserved = redis_set(reservation_key, "1", nx=True, ex=IDEMPOTENCY_TTL_SECONDS)
    except redis.RedisError as err:
        logger.warning("Failed to acquire idempotency reservation for %s: %s", cache_key, err)
        reserved = False

    if not reserved:
        # Another worker may be processing the same request. Try to read the
        # cached response a few times before giving up.
        for _ in range(5):
            cached_payload_raw = redis_get(cache_key)
            if isinstance(cached_payload_raw, (bytes, str)) and cached_payload_raw:
                try:
                    cached_payload = (
                        cached_payload_raw.decode("utf-8") if isinstance(cached_payload_raw, bytes) else cached_payload_raw
                    )
                    cached_entry = json.loads(cached_payload)
                    cached_response = cached_entry.get("response")
                    if cached_response is not None:
                        return cached_response
                except json.JSONDecodeError:
                    redis_delete(cache_key)
                    break
            time.sleep(0.1)
        # If we reach here there is a reservation but no cached response yet.
        # Return a conflict to indicate the request is already in-flight.
        raise HTTPException(status_code=409, detail="Idempotency key conflict: request already in progress.")

    try:
        new_sum = increment_sum(data.number)
    except RedisOverflowError as err:
        raise HTTPException(status_code=400, detail=str(err)) from err
    except RedisUnderflowError as err:
        raise HTTPException(status_code=400, detail=str(err)) from err
    except RedisAtomicError as err:
        logger.error("Atomic increment failed: %s", err)
        raise HTTPException(status_code=500, detail="Failed to update sum due to internal error.") from err
    except RETRYABLE_REDIS_ERRORS as err:
        _raise_redis_unavailable("Could not update the sum.", err)
    except redis.RedisError as err:
        logger.error("Redis error during increment: %s", err)
        raise HTTPException(status_code=500, detail="Failed to update sum due to internal error.") from err
    else:
        response_payload = {"message": "Number added successfully.", "current_sum": new_sum}
        logger.info(
            "abacus.sum.change",
            extra={"event": "abacus.sum.change", "key": SUM_KEY, "delta": data.number, "new_sum": new_sum},
        )
        cache_entry = json.dumps(
            {
                "request": payload_dict,
                "response": response_payload,
                "header_digest": header_digest,
                "cached_at": int(time.time()),
            }
        )
        try:
            stored = redis_set(cache_key, cache_entry, nx=True, ex=IDEMPOTENCY_TTL_SECONDS)
        except redis.RedisError as err:
            logger.error("Failed to store idempotency cache %s: %s", cache_key, err)
            stored = False

        # Release reservation (best-effort). The reservation key has TTL so this
        # is only to speed up other waiters.
        try:
            redis_delete(reservation_key)
        except Exception:
            pass

        if not stored:
            # Another worker stored the cache; return that stored response.
            cached_payload_raw = redis_get(cache_key)
            if isinstance(cached_payload_raw, (bytes, str)) and cached_payload_raw:
                try:
                    cached_payload = (
                        cached_payload_raw.decode("utf-8") if isinstance(cached_payload_raw, bytes) else cached_payload_raw
                    )
                    cached_entry = json.loads(cached_payload)
                    cached_response = cached_entry.get("response")
                    if cached_response is not None:
                        return cached_response
                except json.JSONDecodeError:
                    logger.warning("Failed to decode existing idempotency entry for key %s; overwriting.", cache_key)
                    try:
                        redis_set(cache_key, cache_entry, ex=IDEMPOTENCY_TTL_SECONDS)
                    except Exception:
                        pass

        return response_payload


@app.get("/abacus/sum")
async def get_sum(
    _security: SecurityContext = Depends(security_dependency),
):
    """Retrieves the current running sum from Redis."""
    try:
        current_sum, heal_event = storage_get_sum()
    except RETRYABLE_REDIS_ERRORS as err:
        _raise_redis_unavailable("Could not retrieve the sum.", err)
    except redis.RedisError as err:
        logger.error("Redis error during fetch: %s", err)
        raise HTTPException(status_code=500, detail="Failed to retrieve sum due to internal error.") from err
    else:
        if heal_event:
            logger.warning(heal_event, extra={"event": heal_event, "key": SUM_KEY})
        return {"current_sum": current_sum}


@app.put("/abacus/sum")
async def set_sum(
    data: NumberInput,
    _security: SecurityContext = Depends(security_dependency),
):
    """Sets the running sum in Redis to the provided value."""
    try:
        new_sum = storage_set_sum(data.number)
    except RedisAtomicError as err:
        logger.error("Atomic set failed: %s", err)
        raise HTTPException(status_code=500, detail="Failed to reset sum due to internal error.") from err
    except RETRYABLE_REDIS_ERRORS as err:
        _raise_redis_unavailable("Could not reset the sum.", err)
    except redis.RedisError as err:
        logger.error("Redis error during set: %s", err)
        raise HTTPException(status_code=500, detail="Failed to reset sum due to internal error.") from err
    else:
        logger.info("abacus.sum.reset", extra={"event": "abacus.sum.reset", "key": SUM_KEY, "new_sum": new_sum})
        return {"message": "Sum reset successfully.", "new_sum": new_sum}

