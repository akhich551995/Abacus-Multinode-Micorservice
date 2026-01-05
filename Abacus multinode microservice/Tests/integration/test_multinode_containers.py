import os
import shutil
import subprocess
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor

import pytest
import requests


def _docker_available():
    return shutil.which("docker") is not None


@pytest.mark.integration
def test_two_app_containers_share_redis_and_sum_consistency():
    """Bring up redis + two app containers via docker compose (override with host ports),
    perform concurrent POSTs against both host ports, and assert the final sum matches.
    This test shells out to `docker compose` and requires Docker running on the host.
    """

    if not _docker_available():
        pytest.skip("Docker required for container integration test")

    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    compose_file = os.path.join(repo_root, "docker-compose.yml")

    override_content = """
services:
  app1:
    ports:
      - "8001:8000"
    environment:
      - ABACUS_AUTH_ENABLED=false
      - ABACUS_RATE_LIMIT_ENABLED=false
  app2:
    ports:
      - "8002:8000"
    environment:
      - ABACUS_AUTH_ENABLED=false
      - ABACUS_RATE_LIMIT_ENABLED=false
"""

    tmpdir = tempfile.mkdtemp(prefix="compose-override-")
    override_path = os.path.join(tmpdir, "docker-compose.override.yml")
    try:
        with open(override_path, "w", encoding="utf-8") as fh:
            fh.write(override_content)

        # Bring up the stack with override (build images if needed)
        subprocess.run(["docker", "compose", "-f", compose_file, "-f", override_path, "up", "--build", "-d"], check=True, cwd=repo_root)

        # Wait for the apps to accept connections
        def wait_for(url, timeout=30.0):
            deadline = time.time() + timeout
            while time.time() < deadline:
                try:
                    r = requests.get(url, timeout=2.0)
                    if r.status_code in (200, 400, 401, 403, 404):
                        return True
                except Exception:
                    pass
                time.sleep(0.5)
            return False

        assert wait_for("http://localhost:8001/abacus/sum"), "app1 did not become ready"
        assert wait_for("http://localhost:8002/abacus/sum"), "app2 did not become ready"

        numbers = list(range(1, 101))

        def post_to(port, n, idx):
            url = f"http://localhost:{port}/abacus/number"
            headers = {"Idempotency-Key": f"cont-{idx}"}
            r = requests.post(url, json={"number": n}, headers=headers, timeout=5.0)
            # Accept success or conflict in case of idempotency collisions
            assert r.status_code in (200, 201, 202, 409)

        tasks = []
        with ThreadPoolExecutor(max_workers=20) as ex:
            for i, n in enumerate(numbers):
                port = 8001 if i % 2 == 0 else 8002
                tasks.append(ex.submit(post_to, port, n, i))
            for t in tasks:
                t.result()

        # Give a short grace for writes to persist
        time.sleep(0.5)

        # Read final sum from one of the hosts
        r = requests.get("http://localhost:8001/abacus/sum", timeout=5.0)
        r.raise_for_status()
        data = r.json()
        expected = sum(numbers)
        assert data.get("current_sum") == expected

    finally:
        # Tear down
        subprocess.run(["docker", "compose", "-f", compose_file, "-f", override_path, "down", "-v"], cwd=repo_root)
        try:
            shutil.rmtree(tmpdir)
        except Exception:
            pass
