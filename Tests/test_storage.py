import sys
import os
import pytest

# Ensure the project root is on sys.path so tests can import `src` when pytest
# runs from the repository root. This keeps tests runnable both in CI and locally.
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import json
import time
from fakeredis import FakeRedis

from src import storage


@pytest.fixture(autouse=True)
def fake_redis_client():
    """Provide a fakeredis client and inject it into the storage module.

    This lets tests exercise Redis-backed behavior without a real Redis server.
    """
    client = FakeRedis(decode_responses=True)
    # Provide a minimal `eval` implementation for fakeredis to emulate the
    # Lua scripts used by storage.increment_sum and storage.set_sum.
    # This keeps tests fast and avoids needing a real Redis server.
    def _eval(script: str, numkeys: int, *keys_and_args):
        # keys_and_args contains numkeys keys followed by script args
        sum_key = keys_and_args[0]
        backup_key = keys_and_args[1]

        # Detect increment vs set by looking for a distinctive token
        if "local delta" in script:
            # Increment script
            delta = int(keys_and_args[2])
            min_value = int(keys_and_args[3])
            max_value = int(keys_and_args[4])
            default_value = int(keys_and_args[5])
            backup_ttl = int(keys_and_args[6])
            timestamp = int(keys_and_args[7])

            raw = client.get(sum_key)
            heal_event = None
            raw_value = None
            ttl_before = client.ttl(sum_key)

            if raw is None:
                current = default_value
                client.set(sum_key, str(default_value))
                heal_event = 'abacus.sum.missing'
            else:
                try:
                    current = int(raw)
                except Exception:
                    current = default_value
                    client.set(sum_key, str(default_value))
                    heal_event = 'abacus.sum.corrupted'
                    raw_value = raw

            if delta > 0 and current > (max_value - delta):
                raise redis.exceptions.ResponseError('OVERFLOW')
            if delta < 0 and current < (min_value - delta):
                raise redis.exceptions.ResponseError('UNDERFLOW')

            new_total = current + delta
            client.set(sum_key, str(new_total))
            # persist semantics: ensure no TTL
            try:
                client.persist(sum_key)
            except Exception:
                pass

            backup_payload = json.dumps({"sum": new_total, "updated_at": timestamp})
            if backup_ttl > 0:
                client.set(backup_key, backup_payload, ex=backup_ttl)
            else:
                client.set(backup_key, backup_payload)

            result = {"new_total": new_total}
            if heal_event:
                result["heal_event"] = heal_event
                result["reset_to"] = default_value
            if raw_value:
                result["raw_value"] = raw_value
            if isinstance(ttl_before, int) and ttl_before >= 0:
                result["ttl_before"] = ttl_before

            return json.dumps(result)

        if "local new_value" in script:
            # Set script
            new_value = int(keys_and_args[2])
            min_value = int(keys_and_args[3])
            max_value = int(keys_and_args[4])
            backup_ttl = int(keys_and_args[5])
            timestamp = int(keys_and_args[6])

            if new_value < min_value or new_value > max_value:
                raise redis.exceptions.ResponseError('OUT_OF_RANGE')

            ttl_before = client.ttl(sum_key)
            previous_raw = client.get(sum_key)
            client.set(sum_key, str(new_value))
            try:
                client.persist(sum_key)
            except Exception:
                pass

            backup_payload = json.dumps({"sum": new_value, "updated_at": timestamp})
            if backup_ttl > 0:
                client.set(backup_key, backup_payload, ex=backup_ttl)
            else:
                client.set(backup_key, backup_payload)

            result = {"new_total": new_value}
            if previous_raw is not None:
                try:
                    previous_num = int(previous_raw)
                    result["previous_value"] = previous_num
                except Exception:
                    result["previous_raw"] = previous_raw
            if isinstance(ttl_before, int) and ttl_before >= 0:
                result["ttl_before"] = ttl_before
            return json.dumps(result)

        # Fallback: not implemented
        raise redis.exceptions.ResponseError('EVAL not supported in fake client')

    import redis as _redis_mod
    redis = _redis_mod
    client.eval = _eval
    # Inject into the module under test (storage.redis_client used as override)
    storage.redis_client = client
    # Ensure clean state
    client.flushall()
    yield client
    # Teardown
    client.flushall()
    storage.redis_client = None


def test_increment_and_get_sum():
    # Start from empty state
    storage.redis_delete(storage.SUM_KEY, storage.SUM_BACKUP_KEY)

    new_total = storage.increment_sum(42)
    assert isinstance(new_total, int)
    assert new_total == 42

    current, heal = storage.get_sum()
    assert current == 42
    # After a successful increment, heal event should be None
    assert heal is None


def test_set_sum_and_get_previous():
    storage.redis_delete(storage.SUM_KEY, storage.SUM_BACKUP_KEY)

    new_total = storage.set_sum(100)
    assert new_total == 100

    current, heal = storage.get_sum()
    assert current == 100
    assert heal is None


def test_overflow_and_underflow():
    storage.redis_delete(storage.SUM_KEY, storage.SUM_BACKUP_KEY)

    # Set to max and overflow
    storage.set_sum(storage.MAX_REDIS_INT)
    with pytest.raises(storage.RedisOverflowError):
        storage.increment_sum(1)

    # Set to min and underflow
    storage.set_sum(storage.MIN_REDIS_INT)
    with pytest.raises(storage.RedisUnderflowError):
        storage.increment_sum(-1)
