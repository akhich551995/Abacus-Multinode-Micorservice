import hashlib
import logging
import os
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional

from fastapi import Depends, Header, HTTPException, Request

from . import storage
from .metrics import AUTH_FAILURE_COUNTER, RATE_LIMIT_HIT_COUNTER

logger = logging.getLogger("src.security")


def _to_bool(value: Optional[str], default: bool) -> bool:
    if value is None:
        return default
    value = value.strip().lower()
    if value in {"1", "true", "yes", "on"}:
        return True
    if value in {"0", "false", "no", "off"}:
        return False
    return default


def _parse_api_keys(raw: Optional[str]) -> Dict[str, str]:
    if not raw:
        return {}
    keys = {}
    for entry in raw.split(","):
        token = entry.strip()
        if token:
            digest = hashlib.sha256(token.encode("utf-8")).hexdigest()
            keys[token] = digest
    return keys


@dataclass
class SecuritySettings:
    api_keys: Dict[str, str]
    auth_enabled: bool
    rate_limit_enabled: bool
    rate_limit_requests: int
    rate_limit_window_seconds: int


_settings = SecuritySettings({}, False, False, 0, 60)


def reload_settings() -> None:
    global _settings
    api_keys = _parse_api_keys(os.getenv("ABACUS_API_KEYS"))
    auth_env = os.getenv("ABACUS_AUTH_ENABLED")
    auth_enabled = _to_bool(auth_env, bool(api_keys))

    rl_enabled = _to_bool(os.getenv("ABACUS_RATE_LIMIT_ENABLED"), True)
    rl_requests = int(os.getenv("ABACUS_RATE_LIMIT_REQUESTS", "60"))
    rl_window = int(os.getenv("ABACUS_RATE_LIMIT_WINDOW_SECONDS", "60"))

    _settings = SecuritySettings(
        api_keys=api_keys,
        auth_enabled=auth_enabled,
        rate_limit_enabled=rl_enabled and rl_requests > 0 and rl_window > 0,
        rate_limit_requests=rl_requests,
        rate_limit_window_seconds=max(1, rl_window),
    )


reload_settings()


class SecurityContext:
    __slots__ = ("api_key", "api_key_hash")

    def __init__(self, api_key: Optional[str], api_key_hash: Optional[str]):
        self.api_key = api_key
        self.api_key_hash = api_key_hash


def _masked_key_hash(api_key_hash: Optional[str]) -> str:
    if not api_key_hash:
        return "anonymous"
    return api_key_hash[:12]


async def require_api_key(x_api_key: Optional[str] = Header(None)) -> SecurityContext:
    settings = _settings
    api_key = x_api_key.strip() if isinstance(x_api_key, str) else None

    if not settings.auth_enabled:
        return SecurityContext(api_key=None, api_key_hash=None)

    if not api_key:
        logger.warning("abacus.auth.missing")
        AUTH_FAILURE_COUNTER.labels(reason="missing").inc()
        raise HTTPException(status_code=401, detail="Missing API key.")

    stored_hash = settings.api_keys.get(api_key)
    if not stored_hash:
        logger.warning(
            "abacus.auth.invalid",
            extra={"event": "abacus.auth.invalid", "api_key_hash": hashlib.sha256(api_key.encode("utf-8")).hexdigest()[:12]},
        )
        AUTH_FAILURE_COUNTER.labels(reason="invalid").inc()
        raise HTTPException(status_code=403, detail="Invalid API key.")

    return SecurityContext(api_key=api_key, api_key_hash=stored_hash)


def _rate_limit_key(identifier: str, bucket: int) -> str:
    return f"abacus:rate:{identifier}:{bucket}"


def _execute_with_failover(operation: str, func: Callable[[Any], int]) -> int:
    return storage.execute_with_failover(operation, func)


def enforce_rate_limit(context: SecurityContext, request: Request) -> None:
    settings = _settings
    if not settings.rate_limit_enabled:
        return

    identifier = context.api_key_hash or (request.client.host if request.client else "anonymous")
    log_identifier = identifier if context.api_key_hash is None else _masked_key_hash(context.api_key_hash)
    bucket = int(time.time() // settings.rate_limit_window_seconds)
    key = _rate_limit_key(identifier, bucket)

    def _operation(client: Any) -> int:
        pipeline = client.pipeline()
        pipeline.incr(key, 1)
        pipeline.expire(key, settings.rate_limit_window_seconds)
        results = pipeline.execute()
        count = results[0]
        return int(count)

    try:
        count = _execute_with_failover("rate_limit", _operation)
    except Exception as err:
        logger.error("Rate limiting failed: %s", err)
        raise HTTPException(status_code=503, detail="Service Unavailable: rate limiting failure.") from err

    if count > settings.rate_limit_requests:
        logger.warning(
            "abacus.rate_limit.hit",
            extra={
                "event": "abacus.rate_limit.hit",
                "identifier": log_identifier,
                "count": count,
                "limit": settings.rate_limit_requests,
            },
        )
        RATE_LIMIT_HIT_COUNTER.labels(identifier=log_identifier).inc()
        raise HTTPException(
            status_code=429,
            detail="Rate limit exceeded.",
            headers={"Retry-After": str(settings.rate_limit_window_seconds)},
        )


async def security_dependency(
    request: Request,
    context: SecurityContext = Depends(require_api_key),
) -> SecurityContext:
    enforce_rate_limit(context, request)
    return context