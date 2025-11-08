import importlib

prometheus_client = importlib.import_module("prometheus_client")
Counter = getattr(prometheus_client, "Counter")
Histogram = getattr(prometheus_client, "Histogram")
Gauge = getattr(prometheus_client, "Gauge")
create_metrics_app = getattr(prometheus_client, "make_asgi_app")

HTTP_REQUEST_COUNTER = Counter(
    "abacus_request_total",
    "Total HTTP requests processed by endpoint/method/status.",
    labelnames=("endpoint", "method", "status"),
)

HTTP_REQUEST_LATENCY = Histogram(
    "abacus_request_duration_seconds",
    "Duration of HTTP requests processed by Abacus endpoints.",
    labelnames=("endpoint", "method"),
)

REDIS_OPERATION_COUNTER = Counter(
    "abacus_redis_operations_total",
    "Redis operations executed by the Abacus service, labelled by result.",
    labelnames=("operation", "result"),
)

REDIS_OPERATION_LATENCY = Histogram(
    "abacus_redis_operation_duration_seconds",
    "Latency of Redis operations executed by the Abacus service.",
    labelnames=("operation",),
)

REDIS_RETRY_COUNTER = Counter(
    "abacus_redis_retries_total",
    "Number of Redis retries triggered for an operation.",
    labelnames=("operation",),
)

REDIS_FAILOVER_COUNTER = Counter(
    "abacus_redis_failovers_total",
    "Count of Redis cluster failovers executed for an operation.",
    labelnames=("operation",),
)

REDIS_AVAILABILITY_GAUGE = Gauge(
    "abacus_redis_availability",
    "Redis availability indicator (1=available, 0=unavailable).",
)

SUM_BACKUP_RESTORE_COUNTER = Counter(
    "abacus_sum_backup_restores_total",
    "Total number of times the running sum was restored from backup.",
)

SUM_DISK_RESTORE_COUNTER = Counter(
    "abacus_sum_disk_restores_total",
    "Total number of times the running sum was restored from disk backup.",
)

SUM_TTL_DETECTED_COUNTER = Counter(
    "abacus_sum_ttl_detected_total",
    "Occurrences of TTL detection on the running sum key.",
)

SUM_EVICTION_COUNTER = Counter(
    "abacus_sum_evictions_total",
    "Occurrences of the running sum key being missing or evicted.",
)

RATE_LIMIT_HIT_COUNTER = Counter(
    "abacus_rate_limit_hits_total",
    "Rate limit hits per identifier.",
    labelnames=("identifier",),
)

AUTH_FAILURE_COUNTER = Counter(
    "abacus_auth_failures_total",
    "Authentication failures (missing/invalid API keys).",
    labelnames=("reason",),
)
