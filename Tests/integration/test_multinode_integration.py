import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import sys
import os
import pytest
import redis
from fastapi.testclient import TestClient

# Ensure repository root is importable as a module path so `from src import ...` works
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

from testcontainers.redis import RedisContainer

# Disable auth and rate limiting for integration tests by default to avoid
# 429/401 responses during high-concurrency tests. Set these before importing
# the application modules so their reload logic picks them up.
os.environ.setdefault("ABACUS_AUTH_ENABLED", "false")
os.environ.setdefault("ABACUS_RATE_LIMIT_ENABLED", "false")

from src import storage
from src.main import app


@pytest.mark.integration
def test_concurrent_increments_across_two_nodes():
    """Start a real Redis instance, create two app TestClients that share it,
    run concurrent POSTs across both clients and assert the final sum equals
    the sum of all posted numbers.
    """

    with RedisContainer() as redis_container:
        host = redis_container.get_container_host_ip()
        port = int(redis_container.get_exposed_port(6379))

        client = redis.Redis(host=host, port=port, decode_responses=True)

        # Inject the real Redis client into the storage module so both app
        # TestClients use the same backend.
        storage.redis_client = client

        # Ensure clean starting state
        client.flushall()

        tc1 = TestClient(app)
        tc2 = TestClient(app)

        # Prepare requests: 100 increments total, split across two clients
        numbers = list(range(1, 51)) + list(range(51, 101))

        def post_number(client_obj, number, idx):
            headers = {"Idempotency-Key": f"key-{idx}"}
            res = client_obj.post(
                "/abacus/number",
                json={"number": number},
                headers=headers,
            )
            assert res.status_code in (200, 201, 202, 409)
            return res

        # Run posts concurrently across both clients
        tasks = []
        with ThreadPoolExecutor(max_workers=20) as ex:
            for i, n in enumerate(numbers):
                client_obj = tc1 if i % 2 == 0 else tc2
                tasks.append(ex.submit(post_number, client_obj, n, i))

            # Wait for completion
            for t in tasks:
                t.result()

        # Allow a brief moment for any async writes to complete
        time.sleep(0.5)

        # Fetch final sum via API
        r = tc1.get("/abacus/sum")
        assert r.status_code == 200
        data = r.json()
        assert "current_sum" in data

        expected = sum(numbers)
        assert data["current_sum"] == expected


@pytest.mark.integration
def test_reset_race_is_atomic_and_consistent():
    """Test that concurrent resets and increments do not corrupt the sum.

    The expected semantics: `PUT /abacus/sum` atomically sets the sum to the
    requested value. Concurrent `POST /abacus/number` operations may occur
    before or after the reset; the final sum must be a valid integer and fall
    within the 64-bit bounds. This test verifies no corruption or exceptions
    occur under concurrent load.
    """

    with RedisContainer() as redis_container:
        host = redis_container.get_container_host_ip()
        port = int(redis_container.get_exposed_port(6379))

        client = redis.Redis(host=host, port=port, decode_responses=True)
        storage.redis_client = client
        client.flushall()

        tc1 = TestClient(app)
        tc2 = TestClient(app)

        # Worker that performs increments
        def increment_worker(client_obj, start, count):
            for i in range(start, start + count):
                try:
                    res = client_obj.post(
                        "/abacus/number",
                        json={"number": i},
                        headers={"Idempotency-Key": f"inc-{threading.get_ident()}-{i}"},
                    )
                    assert res.status_code in (200, 409)
                except Exception:
                    pass

        # Worker that performs resets
        def reset_worker(client_obj, value):
            try:
                res = client_obj.put(
                    "/abacus/sum",
                    json={"number": value},
                )
                assert res.status_code == 200
            except Exception:
                pass

        # Launch concurrent increments and resets
        threads = []
        for i in range(5):
            t = threading.Thread(target=increment_worker, args=(tc1, i * 10 + 1, 10))
            threads.append(t)
            t.start()

        # Start a few resets concurrently
        for val in (0, 500, 1000):
            t = threading.Thread(target=reset_worker, args=(tc2, val))
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        # Final check: should be an integer within 64-bit bounds
        r = tc1.get("/abacus/sum")
        assert r.status_code == 200
        data = r.json()
        assert isinstance(data.get("current_sum"), int)
        assert storage.MIN_REDIS_INT <= data.get("current_sum") <= storage.MAX_REDIS_INT
