import json
import logging
import os
import time
from threading import Lock
from typing import Any, Callable, Dict, Optional, Tuple, Type, TypeVar, cast

import redis
from redis.backoff import ExponentialBackoff
from redis.connection import ConnectionPool
from redis.exceptions import (
    BusyLoadingError,
    ClusterDownError,
    ConnectionError,
    ResponseError,
    TimeoutError,
)
from redis.retry import Retry

from src.metrics import (
    REDIS_AVAILABILITY_GAUGE,
    REDIS_FAILOVER_COUNTER,
    REDIS_OPERATION_COUNTER,
    REDIS_OPERATION_LATENCY,
    REDIS_RETRY_COUNTER,
    SUM_BACKUP_RESTORE_COUNTER,
    SUM_DISK_RESTORE_COUNTER,
    SUM_EVICTION_COUNTER,
    SUM_TTL_DETECTED_COUNTER,
)

MAX_REDIS_INT = 2**63 - 1
MIN_REDIS_INT = -(2**63)

# Define the key that will be used to store the running sum in Redis.
SUM_KEY = "abacus:running_sum"
SUM_BACKUP_KEY = "abacus:running_sum:backup"

BACKUP_TTL_SECONDS = int(os.getenv("ABACUS_SUM_BACKUP_TTL_SECONDS", "0"))
_DEFAULT_BACKUP_DIR = os.getenv("ABACUS_DATA_DIR", os.path.join(os.getcwd(), "data"))
BACKUP_FILE_PATH = os.getenv("ABACUS_BACKUP_FILE_PATH", os.path.join(_DEFAULT_BACKUP_DIR, "sum_backup.json"))

# Configure Redis connectivity and retry behaviour.
redis_host = os.getenv("REDIS_HOST", "localhost")
redis_port = int(os.getenv("REDIS_PORT", "6379"))

_REDIS_SOCKET_CONNECT_TIMEOUT = float(os.getenv("REDIS_SOCKET_CONNECT_TIMEOUT", "1"))
_REDIS_SOCKET_TIMEOUT = float(os.getenv("REDIS_SOCKET_TIMEOUT", "1"))
_REDIS_HEALTH_CHECK_INTERVAL = int(os.getenv("REDIS_HEALTH_CHECK_INTERVAL", "30"))
_REDIS_MAX_OPERATION_ATTEMPTS = int(os.getenv("REDIS_MAX_OPERATION_ATTEMPTS", "3"))
_REDIS_BACKOFF_BASE_SECONDS = float(os.getenv("REDIS_BACKOFF_BASE_SECONDS", "0.05"))
_REDIS_BACKOFF_CAP_SECONDS = float(os.getenv("REDIS_BACKOFF_CAP_SECONDS", "0.5"))


logger = logging.getLogger("src.storage")


class RedisClientManager:
    """Builds Redis clients with retry/backoff and manages failover."""

    def __init__(self, host: str, port: int) -> None:
        self._host = host
        self._port = port
        self._lock = Lock()
        self._pool = self._build_pool(host, port)

    def _build_pool(self, host: str, port: int) -> ConnectionPool:
        retry_strategy = Retry(
            ExponentialBackoff(cap=_REDIS_BACKOFF_CAP_SECONDS, base=_REDIS_BACKOFF_BASE_SECONDS),
            _REDIS_MAX_OPERATION_ATTEMPTS,
        )
        return ConnectionPool(
            host=host,
            port=port,
            db=0,
            decode_responses=True,
            socket_connect_timeout=_REDIS_SOCKET_CONNECT_TIMEOUT,
            socket_timeout=_REDIS_SOCKET_TIMEOUT,
            health_check_interval=_REDIS_HEALTH_CHECK_INTERVAL,
            socket_keepalive=True,
            retry=retry_strategy,
        )

    def get_client(self) -> redis.Redis:
        with self._lock:
            pool = self._pool
        return redis.Redis(connection_pool=pool, decode_responses=True)

    def failover(self, host: str, port: int) -> None:
        with self._lock:
            if host == self._host and port == self._port:
                return
            self._host = host
            self._port = port
            self._pool = self._build_pool(host, port)


_redis_manager = RedisClientManager(redis_host, redis_port)

# Used as a simple override in tests.
redis_client: Optional[redis.Redis] = None


T = TypeVar("T")


def _backoff_sleep(attempt: int) -> None:
    delay = min(_REDIS_BACKOFF_CAP_SECONDS, _REDIS_BACKOFF_BASE_SECONDS * (2 ** (attempt - 1)))
    if delay > 0:
        time.sleep(delay)


def _parse_cluster_redirect(message: str) -> Optional[Tuple[str, int]]:
    parts = message.split()
    if len(parts) >= 3 and parts[0] in {"MOVED", "ASK"}:
        host_port = parts[2]
        if ":" in host_port:
            host, port_str = host_port.rsplit(":", 1)
            try:
                return host, int(port_str)
            except ValueError:
                return None
    return None


RETRYABLE_ERRORS: Tuple[Type[BaseException], ...] = (
    BusyLoadingError,
    ClusterDownError,
    ConnectionError,
    TimeoutError,
)


def _persist_sum_key(client: redis.Redis) -> None:
    try:
        client.persist(SUM_KEY)
    except redis.RedisError as err:
        logger.warning("Failed to persist %s: %s", SUM_KEY, err)


def _write_backup_with_client(client: redis.Redis, total: int) -> None:
    timestamp = int(time.time())
    payload_dict = {"sum": total, "updated_at": timestamp}
    payload = json.dumps(payload_dict)
    try:
        if BACKUP_TTL_SECONDS > 0:
            client.set(SUM_BACKUP_KEY, payload, ex=BACKUP_TTL_SECONDS)
        else:
            client.set(SUM_BACKUP_KEY, payload)
    except redis.RedisError as err:
        logger.warning("Failed to snapshot %s: %s", SUM_BACKUP_KEY, err)
    _write_backup_to_disk(payload_dict)


def _write_backup_to_disk(payload: Dict[str, Any]) -> None:
    if not BACKUP_FILE_PATH:
        return
    try:
        os.makedirs(os.path.dirname(BACKUP_FILE_PATH), exist_ok=True)
        with open(BACKUP_FILE_PATH, "w", encoding="utf-8") as fh:
            json.dump(payload, fh)
    except OSError as err:
        logger.warning("Failed to write disk backup %s: %s", BACKUP_FILE_PATH, err)


def _load_backup(client: redis.Redis) -> Optional[Tuple[int, Dict[str, Any]]]:
    raw: Any = client.get(SUM_BACKUP_KEY)
    if raw is None:
        return None
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8", errors="ignore")
    metadata: Dict[str, Any] = {}
    if isinstance(raw, str):
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            data = None
        if isinstance(data, dict) and "sum" in data:
            try:
                value = int(data["sum"])
            except (TypeError, ValueError):
                return None
            metadata = data
            return value, metadata
        try:
            value = int(cast(Any, raw))
        except (TypeError, ValueError):
            return None
        metadata = {"sum": value}
        return value, metadata
    try:
        value = int(cast(Any, raw))
    except (TypeError, ValueError):
        return None
    metadata = {"sum": value}
    return value, metadata


def _load_backup_from_disk() -> Optional[Tuple[int, Dict[str, Any]]]:
    if not BACKUP_FILE_PATH:
        return None
    try:
        with open(BACKUP_FILE_PATH, "r", encoding="utf-8") as fh:
            data = json.load(fh)
    except FileNotFoundError:
        return None
    except (OSError, json.JSONDecodeError) as err:
        logger.warning("Failed to read disk backup %s: %s", BACKUP_FILE_PATH, err)
        return None

    if isinstance(data, dict) and "sum" in data:
        try:
            value = int(data["sum"])
        except (TypeError, ValueError):
            logger.warning("Invalid sum value in disk backup %s", BACKUP_FILE_PATH)
            return None
        metadata = dict(data)
        metadata.setdefault("sum", value)
        return value, metadata

    logger.warning("Unexpected payload in disk backup %s", BACKUP_FILE_PATH)
    return None


def _restore_from_backup_with_client(client: redis.Redis) -> Optional[int]:
    backup_source = "redis"
    backup = _load_backup(client)
    if backup is None:
        backup = _load_backup_from_disk()
        if backup is None:
            return None
        backup_source = "disk"
    value, metadata = backup
    try:
        client.set(SUM_KEY, str(value))
    except redis.RedisError as err:
        logger.error("Failed to restore %s from %s backup: %s", SUM_KEY, backup_source, err)
        return None

    if backup_source == "redis":
        SUM_BACKUP_RESTORE_COUNTER.inc()
    else:
        SUM_DISK_RESTORE_COUNTER.inc()

    _write_backup_with_client(client, value)
    _log_audit_event(
        "abacus.sum.restore_from_backup",
        key=SUM_KEY,
        backup_value=value,
        backup_updated_at=metadata.get("updated_at"),
        backup_source=backup_source,
    )
    return value


def _current_ttl(client: redis.Redis) -> Optional[int]:
    try:
        ttl_value = cast(Optional[int], client.ttl(SUM_KEY))
        return ttl_value
    except redis.RedisError as err:
        logger.warning("Failed to fetch TTL for %s: %s", SUM_KEY, err)
        return None


def _record_sum_state(client: redis.Redis, total: int) -> None:
    ttl = _current_ttl(client)
    if ttl is not None and ttl >= 0:
        _log_audit_event("abacus.sum.ttl_detected", key=SUM_KEY, ttl=ttl)
        SUM_TTL_DETECTED_COUNTER.inc()
    _write_backup_with_client(client, total)
    _persist_sum_key(client)


def _execute_with_failover(operation: str, func: Callable[[redis.Redis], T]) -> T:
    last_error: Optional[BaseException] = None
    for attempt in range(1, _REDIS_MAX_OPERATION_ATTEMPTS + 1):
        client = get_redis_client()
        start_time = time.perf_counter()
        try:
            result = func(client)
        except RETRYABLE_ERRORS as err:
            duration = time.perf_counter() - start_time
            REDIS_OPERATION_LATENCY.labels(operation=operation).observe(duration)
            last_error = err
            _log_audit_event(
                "abacus.redis.retry",
                operation=operation,
                attempt=attempt,
                error=str(err),
            )
            REDIS_RETRY_COUNTER.labels(operation=operation).inc()
        except ResponseError as err:
            duration = time.perf_counter() - start_time
            REDIS_OPERATION_LATENCY.labels(operation=operation).observe(duration)
            redirect = _parse_cluster_redirect(str(err))
            if redirect:
                host, port = redirect
                _redis_manager.failover(host, port)
                _log_audit_event(
                    "abacus.redis.failover",
                    operation=operation,
                    host=host,
                    port=port,
                )
                REDIS_FAILOVER_COUNTER.labels(operation=operation).inc()
                continue
            REDIS_OPERATION_COUNTER.labels(operation=operation, result="failure").inc()
            REDIS_AVAILABILITY_GAUGE.set(0)
            raise
        except redis.RedisError as err:
            duration = time.perf_counter() - start_time
            REDIS_OPERATION_LATENCY.labels(operation=operation).observe(duration)
            REDIS_OPERATION_COUNTER.labels(operation=operation, result="failure").inc()
            REDIS_AVAILABILITY_GAUGE.set(0)
            raise
        else:
            duration = time.perf_counter() - start_time
            REDIS_OPERATION_LATENCY.labels(operation=operation).observe(duration)
            REDIS_OPERATION_COUNTER.labels(operation=operation, result="success").inc()
            REDIS_AVAILABILITY_GAUGE.set(1)
            return result

        if attempt < _REDIS_MAX_OPERATION_ATTEMPTS:
            _backoff_sleep(attempt)
            continue

        if last_error is not None:
            _log_audit_event(
                "abacus.redis.fail",
                operation=operation,
                error=str(last_error),
            )
            REDIS_OPERATION_COUNTER.labels(operation=operation, result="failure").inc()
            REDIS_AVAILABILITY_GAUGE.set(0)
            raise last_error

    # Should never reach here because the loop either returns or raises.
    raise RuntimeError(f"Operation {operation} exhausted without raising explicit error.")


def execute_with_failover(operation: str, func: Callable[[redis.Redis], T]) -> T:
    """Expose failover execution for other modules (e.g., rate limiting)."""
    return _execute_with_failover(operation, func)


def get_redis_client() -> redis.Redis:
    """Returns the configured Redis client instance."""
    if redis_client is not None:
        return cast(redis.Redis, redis_client)
    return _redis_manager.get_client()


class RedisAtomicError(RuntimeError):
    """Base error for atomic Redis operations."""


class RedisOverflowError(RedisAtomicError):
    """Raised when an increment would overflow the 64-bit range."""


class RedisUnderflowError(RedisAtomicError):
    """Raised when an increment would underflow the 64-bit range."""


_ATOMIC_INCREMENT_LUA = """
local sum_key = KEYS[1]
local backup_key = KEYS[2]

local delta = tonumber(ARGV[1])
local min_value = tonumber(ARGV[2])
local max_value = tonumber(ARGV[3])
local default_value = tonumber(ARGV[4])
local backup_ttl = tonumber(ARGV[5])
local timestamp = tonumber(ARGV[6])

local ttl_before = redis.call('TTL', sum_key)
local current_raw = redis.call('GET', sum_key)
local heal_event = nil
local raw_value = nil
local current

if not current_raw then
        current = default_value
        redis.call('SET', sum_key, default_value)
        heal_event = 'abacus.sum.missing'
else
        current = tonumber(current_raw)
        if not current then
                current = default_value
                redis.call('SET', sum_key, default_value)
                heal_event = 'abacus.sum.corrupted'
                raw_value = current_raw
        end
end

if delta > 0 and current > (max_value - delta) then
    return redis.error_reply('OVERFLOW')
end

if delta < 0 and current < (min_value - delta) then
    return redis.error_reply('UNDERFLOW')
end

local new_total = current + delta
redis.call('SET', sum_key, new_total)
redis.call('PERSIST', sum_key)

local backup_payload = cjson.encode({ sum = new_total, updated_at = timestamp })
if backup_ttl > 0 then
        redis.call('SET', backup_key, backup_payload, 'EX', backup_ttl)
else
        redis.call('SET', backup_key, backup_payload)
end

local result = { new_total = new_total }
if heal_event then
        result['heal_event'] = heal_event
        result['reset_to'] = default_value
end
if raw_value then
        result['raw_value'] = raw_value
end
if ttl_before and ttl_before >= 0 then
        result['ttl_before'] = ttl_before
end

return cjson.encode(result)
"""


_ATOMIC_SET_LUA = """
local sum_key = KEYS[1]
local backup_key = KEYS[2]

local new_value = tonumber(ARGV[1])
local min_value = tonumber(ARGV[2])
local max_value = tonumber(ARGV[3])
local backup_ttl = tonumber(ARGV[4])
local timestamp = tonumber(ARGV[5])

if not new_value then
    return redis.error_reply('NON_NUMERIC_VALUE')
end

if new_value < min_value or new_value > max_value then
    return redis.error_reply('OUT_OF_RANGE')
end

local ttl_before = redis.call('TTL', sum_key)
local previous_raw = redis.call('GET', sum_key)
redis.call('SET', sum_key, new_value)
redis.call('PERSIST', sum_key)

local backup_payload = cjson.encode({ sum = new_value, updated_at = timestamp })
if backup_ttl > 0 then
        redis.call('SET', backup_key, backup_payload, 'EX', backup_ttl)
else
        redis.call('SET', backup_key, backup_payload)
end

local result = { new_total = new_value }
if previous_raw then
    local previous_num = tonumber(previous_raw)
    if previous_num then
        result['previous_value'] = previous_num
    else
        result['previous_raw'] = previous_raw
    end
end
if ttl_before and ttl_before >= 0 then
    result['ttl_before'] = ttl_before
end

return cjson.encode(result)
"""


def _log_audit_event(event: str, **kwargs: Any) -> None:
    logger.info("%s", event, extra={"event": event, **kwargs})


def _decode_atomic_result(raw_result: Any) -> Dict[str, Any]:
    if isinstance(raw_result, dict):
        return raw_result
    if isinstance(raw_result, bytes):
        raw_result = raw_result.decode("utf-8")
    if isinstance(raw_result, str):
        try:
            data = json.loads(raw_result)
        except json.JSONDecodeError:
            return {"new_total": raw_result}
        if isinstance(data, dict):
            return data
        return {"new_total": data}
    return {"new_total": raw_result}


def _ensure_sum_integrity(default: int = 0) -> Tuple[int, Optional[str]]:
    def _operation(client: redis.Redis) -> Tuple[int, Optional[str]]:
        raw = client.get(SUM_KEY)

        if raw is None:
            SUM_EVICTION_COUNTER.inc()
            restored = _restore_from_backup_with_client(client)
            if restored is not None:
                return restored, "abacus.sum.restore_from_backup"
            client.set(SUM_KEY, str(default))
            _log_audit_event("abacus.sum.missing", key=SUM_KEY, reset_to=default)
            _record_sum_state(client, default)
            return default, "abacus.sum.missing"

        if isinstance(raw, bytes):
            raw_str = raw.decode("utf-8", errors="ignore")
        else:
            raw_str = str(raw)

        try:
            current = int(raw_str)
        except (TypeError, ValueError):
            client.set(SUM_KEY, str(default))
            _log_audit_event("abacus.sum.corrupted", key=SUM_KEY, raw_value=raw_str, reset_to=default)
            _record_sum_state(client, default)
            return default, "abacus.sum.corrupted"

        ttl = _current_ttl(client)
        if ttl is not None and ttl >= 0:
            _record_sum_state(client, current)
            return current, "abacus.sum.ttl_detected"

        return current, None

    return _execute_with_failover("get_sum", _operation)


def increment_sum(delta: int, *, min_value: int = MIN_REDIS_INT, max_value: int = MAX_REDIS_INT) -> int:
    """Atomically increments the running sum while enforcing 64-bit bounds."""
    def _operation(client: redis.Redis) -> Dict[str, Any]:
        atomic_increment = getattr(client, "atomic_increment", None)
        if callable(atomic_increment):
            result = atomic_increment(SUM_KEY, delta, min_value, max_value)
            return _decode_atomic_result(result)

        try:
            result: Any = client.eval(
                _ATOMIC_INCREMENT_LUA,
                2,
                SUM_KEY,
                SUM_BACKUP_KEY,
                str(delta),
                str(min_value),
                str(max_value),
                "0",
                str(BACKUP_TTL_SECONDS),
                str(int(time.time())),
            )
            return _decode_atomic_result(result)
        except ResponseError as err:
            message = str(err)
            if "OVERFLOW" in message:
                raise RedisOverflowError("Integer overflow: The resulting sum would exceed the 64-bit integer limit.") from err
            if "UNDERFLOW" in message:
                raise RedisUnderflowError("Integer underflow: The resulting sum would be less than the 64-bit integer limit.") from err
            raise

    data = _execute_with_failover("increment_sum", _operation)

    new_total = int(str(data.get("new_total", 0)))
    heal_event = data.get("heal_event")
    if heal_event:
        metadata: Dict[str, Any] = {"key": SUM_KEY}
        if "reset_to" in data:
            metadata["reset_to"] = data["reset_to"]
        if "raw_value" in data:
            metadata["raw_value"] = data["raw_value"]
        _log_audit_event(str(heal_event), **metadata)

    ttl_before = data.get("ttl_before")
    if isinstance(ttl_before, (int, float)) and ttl_before >= 0:
        _log_audit_event("abacus.sum.ttl_detected", key=SUM_KEY, ttl=int(ttl_before))
        SUM_TTL_DETECTED_COUNTER.inc()

    _log_audit_event("abacus.sum.change", key=SUM_KEY, delta=delta, new_total=new_total)
    return new_total


def get_sum(default: int = 0) -> Tuple[int, Optional[str]]:
    """
    Return the current running sum, healing missing or corrupt values when detected.

    Returns a tuple of (current_sum, heal_event) where heal_event is a string such as
    "abacus.sum.missing" or "abacus.sum.corrupted" when auto-healing occurred, otherwise None.
    """
    return _ensure_sum_integrity(default)


def set_sum(value: int, *, min_value: int = MIN_REDIS_INT, max_value: int = MAX_REDIS_INT) -> int:
    """Atomically sets the running sum while enforcing 64-bit bounds."""
    def _operation(client: redis.Redis) -> Dict[str, Any]:
        atomic_set = getattr(client, "atomic_set", None)
        if callable(atomic_set):
            result = atomic_set(SUM_KEY, value, min_value, max_value)
            return _decode_atomic_result(result)

        try:
            result: Any = client.eval(
                _ATOMIC_SET_LUA,
                2,
                SUM_KEY,
                SUM_BACKUP_KEY,
                str(value),
                str(min_value),
                str(max_value),
                str(BACKUP_TTL_SECONDS),
                str(int(time.time())),
            )
            return _decode_atomic_result(result)
        except ResponseError as err:
            message = str(err)
            if "OUT_OF_RANGE" in message or "NON_NUMERIC" in message:
                raise RedisAtomicError("Invalid sum value provided for set operation.") from err
            raise

    data = _execute_with_failover("set_sum", _operation)

    new_total = int(str(data.get("new_total", value)))
    ttl_before = data.get("ttl_before")
    if isinstance(ttl_before, (int, float)) and ttl_before >= 0:
        _log_audit_event("abacus.sum.ttl_detected", key=SUM_KEY, ttl=int(ttl_before))
        SUM_TTL_DETECTED_COUNTER.inc()
    _log_audit_event("abacus.sum.reset", key=SUM_KEY, new_total=new_total)
    return new_total


def redis_ping() -> bool:
    return bool(_execute_with_failover("ping", lambda client: client.ping()))


def redis_setnx(key: str, value: Any) -> bool:
    return bool(_execute_with_failover("setnx", lambda client: client.setnx(key, value)))


def redis_get(key: str) -> Any:
    return _execute_with_failover("get", lambda client: client.get(key))


def redis_set(key: str, value: Any, **kwargs: Any) -> bool:
    return bool(_execute_with_failover("set", lambda client: client.set(key, value, **kwargs)))


def redis_delete(*keys: str) -> int:
    result = _execute_with_failover("delete", lambda client: client.delete(*keys))
    if result is None:
        return 0
    try:
        return int(cast(Any, result))
    except (TypeError, ValueError):
        return 0
