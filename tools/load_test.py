"""Simple load test script using httpx to drive concurrent POSTs and poll /metrics.

Usage:
  python tools/load_test.py --target1 http://localhost:8001 --target2 http://localhost:8002 \
      --requests 1000 --concurrency 50 --metrics-url http://localhost:8001/metrics

The script will fire `--requests` POST requests split across the two targets and
poll the metrics endpoint at `--metrics-url` every `--metrics-interval` seconds
while the load test runs. Metrics snapshots are written to `metrics_snapshots.txt`.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import time
from typing import List

import httpx


async def worker(worker_id: int, queue: asyncio.Queue, client: httpx.AsyncClient, results: List[dict], token_queue: asyncio.Queue | None = None):
    while True:
        item = await queue.get()
        if item is None:
            queue.task_done()
            break
        url, payload, headers = item
        # throttle globally if token_queue is provided
        if token_queue is not None:
            await token_queue.get()
        start = time.time()
        try:
            r = await client.post(url, json=payload, headers=headers, timeout=30.0)
            elapsed = time.time() - start
            results.append({
                "ts": time.time(),
                "worker": worker_id,
                "url": url,
                "status": r.status_code,
                "elapsed": elapsed,
                "error": "",
            })
        except Exception as e:
            results.append({
                "ts": time.time(),
                "worker": worker_id,
                "url": url,
                "status": None,
                "elapsed": None,
                "error": str(e),
            })
        finally:
            queue.task_done()


async def metrics_poller(url: str, interval: float, out_list: List[str], stop_event: asyncio.Event):
    async with httpx.AsyncClient() as client:
        while not stop_event.is_set():
            try:
                r = await client.get(url, timeout=5.0)
                out_list.append(r.text)
            except Exception as e:
                out_list.append(f"#ERROR: {e}")
            await asyncio.sleep(interval)


async def run_load(targets: List[str], total_requests: int, concurrency: int, metrics_url: str, metrics_interval: float, use_idempotency: bool, api_key: str | None = None, rate: float = 0.0, out_csv: str | None = None):
    queue: asyncio.Queue = asyncio.Queue()
    results: List[dict] = []
    metrics_snapshots: List[str] = []
    stop_event = asyncio.Event()
    token_queue: asyncio.Queue | None = None
    token_producer = None

    async with httpx.AsyncClient() as client:
        # prepare global rate limiter if requested
        if rate and rate > 0.0:
            token_queue = asyncio.Queue()

            async def token_producer_fn(q: asyncio.Queue, total: int, rps: float):
                interval = 1.0 / float(rps)
                for _ in range(total):
                    await q.put(1)
                    await asyncio.sleep(interval)

            token_producer = asyncio.create_task(token_producer_fn(token_queue, total_requests, rate))

        # enqueue requests
        for i in range(total_requests):
            target = targets[i % len(targets)]
            url = target.rstrip("/") + "/abacus/number"
            payload = {"number": 1}
            headers = {}
            if use_idempotency:
                headers["Idempotency-Key"] = f"load-{i}"
            if api_key:
                headers["X-API-Key"] = api_key
            await queue.put((url, payload, headers))

        # start workers
        workers = [asyncio.create_task(worker(i, queue, client, results, token_queue)) for i in range(concurrency)]

        # start metrics poller
        poller = asyncio.create_task(metrics_poller(metrics_url, metrics_interval, metrics_snapshots, stop_event))

        start = time.time()
        # wait for queue to be processed
        await queue.join()
        # stop workers
        for _ in workers:
            await queue.put(None)
        await asyncio.gather(*workers)

        # stop metrics poller
        stop_event.set()
        await poller

        duration = time.time() - start

    # Summarize results
    successes = sum(1 for r in results if isinstance(r, dict) and r.get("status") and r.get("status") < 400)
    failures = len(results) - successes
    print(f"Completed {len(results)} requests in {duration:.2f}s — {successes} successful, {failures} failed")

    # write metrics snapshots (always write a file)
    with open("metrics_snapshots.txt", "w", encoding="utf-8") as fh:
        if metrics_snapshots:
            for snap in metrics_snapshots:
                fh.write("# --- snapshot ---\n")
                fh.write(snap)
                fh.write("\n\n")
            print(f"Wrote {len(metrics_snapshots)} metrics snapshots to metrics_snapshots.txt")
        else:
            fh.write("# --- no snapshots collected ---\n")
            print("No metrics snapshots collected; wrote placeholder metrics_snapshots.txt")

    # write per-request CSV if requested
    if out_csv:
        import csv

        with open(out_csv, "w", newline="", encoding="utf-8") as fh:
            writer = csv.writer(fh)
            writer.writerow(["ts", "worker", "url", "status", "elapsed", "error"])
            for r in results:
                writer.writerow([
                    r.get("ts"),
                    r.get("worker"),
                    r.get("url"),
                    r.get("status"),
                    r.get("elapsed"),
                    r.get("error"),
                ])
        print(f"Wrote per-request results to {out_csv}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--target1", required=True)
    parser.add_argument("--target2", required=False)
    parser.add_argument("--requests", type=int, default=1000)
    parser.add_argument("--concurrency", type=int, default=50)
    parser.add_argument("--metrics-url", required=False, default=None)
    parser.add_argument("--metrics-interval", type=float, default=1.0)
    parser.add_argument("--idempotency", action="store_true", help="Enable Idempotency-Key usage for requests")
    parser.add_argument("--api-key", required=False, help="API key to send as X-API-Key header")
    parser.add_argument("--rate", type=float, required=False, default=0.0, help="Global request rate (requests per second). 0 = no limit")
    parser.add_argument("--out-csv", required=False, default=None, help="Path to write per-request CSV results")
    args = parser.parse_args()

    targets = [args.target1]
    if args.target2:
        targets.append(args.target2)

    metrics_url = args.metrics_url or (targets[0].rstrip("/") + "/metrics")

    asyncio.run(run_load(targets, args.requests, args.concurrency, metrics_url, args.metrics_interval, args.idempotency, args.api_key, args.rate, args.out_csv))


if __name__ == "__main__":
    main()
