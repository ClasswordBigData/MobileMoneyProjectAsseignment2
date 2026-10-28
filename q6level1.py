
import os
import csv
import time
import math
import queue
import statistics
import multiprocessing as mp

from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo

import psycopg
from neo4j import GraphDatabase


# ============================================================
# CONFIGURATION
# ============================================================

BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "benchmark_results"
OUTPUT_DIR.mkdir(exist_ok=True)

PG_DSN = os.getenv(
    "PG_DSN",
    "host=localhost port=5432 "
    "dbname=mobile_money_investigations user=postgres"
)

NEO_URI = os.getenv("NEO_URI", "bolt://localhost:7687")
NEO_USER = os.getenv("NEO_USER", "neo4j")
NEO_PASSWORD = "kigalirwanda"

if not NEO_PASSWORD:
    raise RuntimeError(
        "Set NEO_PASSWORD in PyCharm Run Configuration."
    )

NEO_DATABASE = "mobilemoney"
TIMEZONE = ZoneInfo("Africa/Kigali")

CASES = [
    ("A", 998, 24755),
    ("B", 147228, 186823),
    ("C", 206978, 6573),
]

CLIENT_COUNTS = [1, 4, 8, 16]
REPETITIONS = 3
TEST_SECONDS = 15
QUERY_TIMEOUT_SECONDS = 60
READY_TIMEOUT_SECONDS = 180

QUERY_FILES = {
    ("PostgreSQL", "Q4"): BASE_DIR / "pg_q4.sql",
    ("PostgreSQL", "Q7"): BASE_DIR / "pg_q7.sql",
    ("Neo4j", "Q4"): BASE_DIR / "neo_q4.cypher",
    ("Neo4j", "Q7"): BASE_DIR / "neo_q7.cypher",
}


# ============================================================
# QUERY LOADING
# ============================================================

def load_queries():
    queries = {}

    for key, path in QUERY_FILES.items():
        if not path.is_file():
            raise FileNotFoundError(
                f"Missing query file: {path}"
            )

        query = path.read_text(encoding="utf-8").strip()

        if not query:
            raise ValueError(f"Query file is empty: {path}")

        queries[key] = query

    return queries


# ============================================================
# DATABASE CONNECTIONS
# ============================================================

def connect_postgres():
    conn = psycopg.connect(
        PG_DSN,
        connect_timeout=15,
    )

    conn.autocommit = True

    conn.execute(
        "SET TIME ZONE 'Africa/Kigali'"
    )

    conn.execute(
        f"SET statement_timeout = '{QUERY_TIMEOUT_SECONDS}s'"
    )

    return conn


def connect_neo4j():
    driver = GraphDatabase.driver(
        NEO_URI,
        auth=(NEO_USER, NEO_PASSWORD),
        connection_timeout=15,
        max_connection_pool_size=1,
    )

    driver.verify_connectivity()

    return driver


# ============================================================
# QUERY EXECUTION
# ============================================================

def execute_query(
    engine,
    workload,
    query,
    review,
    source,
    destination,
    pg_conn,
    neo_session,
):
    started = time.perf_counter()

    started_cat = datetime.now(TIMEZONE).isoformat(
        timespec="milliseconds"
    )

    status = "success"
    error_message = ""
    row_count = 0
    rows = []

    params = {
        "review": review,
        "source": source,
        "destination": destination,
        "max_depth": 10 if workload == "Q4" else 6,
    }

    try:
        if engine == "PostgreSQL":
            with pg_conn.cursor() as cursor:
                cursor.execute(query, params)

                if cursor.description:
                    rows = cursor.fetchall()

        else:
            result = neo_session.run(query, params)
            rows = list(result)

            # Consume the complete result so latency includes
            # server execution and result transfer.
            result.consume()

        row_count = len(rows)

    except Exception as exc:
        message = str(exc)
        error_message = message[:1500]

        lowered = message.lower()

        if (
            "timeout" in lowered
            or "timed out" in lowered
            or "deadline" in lowered
            or "canceling statement" in lowered
        ):
            status = "timeout"
        else:
            status = "error"

    finished = time.perf_counter()

    return {
        "engine": engine,
        "workload": workload,
        "review": review,
        "source": source,
        "destination": destination,
        "start_cat": started_cat,
        "latency_ms": round(
            (finished - started) * 1000, 3
        ),
        "row_count": row_count,
        "status": status,
        "error": error_message,
        "finish_perf": finished,
    }


# ============================================================
# MULTIPROCESSING WORKER
# ============================================================

def worker(
    engine,
    workload,
    worker_id,
    query,
    ready_queue,
    start_event,
    release_time,
    deadline_time,
    log_queue,
):
    pg_conn = None
    driver = None
    neo_session = None

    try:
        # One persistent database connection per worker.
        if engine == "PostgreSQL":
            pg_conn = connect_postgres()
        else:
            driver = connect_neo4j()
            neo_session = driver.session(
                database=NEO_DATABASE
            )

        # Warm-up: run A, B and C once before timing.
        for review, source, destination in CASES:
            warmup = execute_query(
                engine,
                workload,
                query,
                review,
                source,
                destination,
                pg_conn,
                neo_session,
            )

            if warmup["status"] != "success":
                raise RuntimeError(
                    "Warm-up failed for "
                    f"{review}: {warmup['error']}"
                )

        # Tell coordinator this worker is ready.
        ready_queue.put({
            "worker_id": worker_id,
            "status": "ready",
        })

        # Wait for all clients to be ready.
        if not start_event.wait(
            timeout=READY_TIMEOUT_SECONDS
        ):
            raise TimeoutError(
                "Timed out waiting for benchmark start."
            )

        start = release_time.value
        deadline = deadline_time.value

        case_index = 0

        # Finish a query once started, even if it crosses
        # the 15-second deadline.
        while time.perf_counter() < deadline:
            review, source, destination = CASES[
                case_index % len(CASES)
            ]
            case_index += 1

            result = execute_query(
                engine,
                workload,
                query,
                review,
                source,
                destination,
                pg_conn,
                neo_session,
            )

            result.update({
                "worker_id": worker_id,
                "release_perf": start,
                "deadline_perf": deadline,
                "warmup": False,
            })

            log_queue.put(result)

        log_queue.put({
            "engine": engine,
            "workload": workload,
            "worker_id": worker_id,
            "status": "worker_finished",
            "finish_perf": time.perf_counter(),
        })

    except Exception as exc:
        log_queue.put({
            "engine": engine,
            "workload": workload,
            "worker_id": worker_id,
            "status": "worker_error",
            "error": str(exc)[:1500],
            "finish_perf": time.perf_counter(),
        })

        # Avoid leaving the coordinator waiting forever
        # for a worker that failed before becoming ready.
        ready_queue.put({
            "worker_id": worker_id,
            "status": "failed",
            "error": str(exc)[:1500],
        })

    finally:
        if neo_session is not None:
            neo_session.close()

        if driver is not None:
            driver.close()

        if pg_conn is not None:
            pg_conn.close()


# ============================================================
# STATISTICS
# ============================================================

def percentile_nearest_rank(values, percentile):
    if not values:
        return None

    ordered = sorted(values)

    rank = max(
        1,
        math.ceil(percentile * len(ordered))
    )

    return ordered[rank - 1]


def summarize_run(
    engine,
    workload,
    clients,
    repetition,
    results,
    worker_finish_times,
    release_perf,
):
    requests = [
        row for row in results
        if row.get("status") in (
            "success", "error", "timeout"
        )
    ]

    successful = [
        row for row in requests
        if row["status"] == "success"
    ]

    errors = sum(
        row["status"] == "error"
        for row in requests
    )

    timeouts = sum(
        row["status"] == "timeout"
        for row in requests
    )

    latencies = [
        row["latency_ms"] for row in successful
    ]

    if worker_finish_times:
        final_finish = max(worker_finish_times)
    else:
        final_finish = time.perf_counter()

    elapsed_seconds = max(
        final_finish - release_perf,
        0.001,
    )

    return {
        "engine": engine,
        "workload": workload,
        "clients": clients,
        "repetition": repetition,
        "requests": len(requests),
        "successes": len(successful),
        "errors": errors,
        "timeouts": timeouts,
        "throughput_rps": round(
            len(successful) / elapsed_seconds, 3
        ),
        "median_latency_ms": (
            round(statistics.median(latencies), 3)
            if latencies else None
        ),
        "p95_latency_ms": (
            round(
                percentile_nearest_rank(
                    latencies, 0.95
                ),
                3,
            )
            if latencies else None
        ),
        "elapsed_seconds": round(
            elapsed_seconds, 3
        ),
    }


# ============================================================
# CSV OUTPUT
# ============================================================

def write_csv(path, rows):
    if not rows:
        print(f"No rows to write: {path}")
        return

    # Include keys seen across all records.
    fieldnames = list(dict.fromkeys(
        key for row in rows for key in row.keys()
    ))

    with path.open(
        "w",
        newline="",
        encoding="utf-8-sig",
    ) as file:
        writer = csv.DictWriter(
            file,
            fieldnames=fieldnames,
            extrasaction="ignore",
        )
        writer.writeheader()
        writer.writerows(rows)


# ============================================================
# ONE BENCHMARK REPETITION
# ============================================================

def run_one(
    engine,
    workload,
    clients,
    repetition,
    query,
):
    print(
        f"\nStarting {engine} {workload}: "
        f"clients={clients}, repetition={repetition}"
    )

    ctx = mp.get_context("spawn")

    ready_queue = ctx.Queue()
    log_queue = ctx.Queue()

    start_event = ctx.Event()

    release_time = ctx.Value("d", 0.0)
    deadline_time = ctx.Value("d", 0.0)

    processes = []

    for worker_id in range(clients):
        process = ctx.Process(
            target=worker,
            args=(
                engine,
                workload,
                worker_id,
                query,
                ready_queue,
                start_event,
                release_time,
                deadline_time,
                log_queue,
            ),
        )

        process.start()
        processes.append(process)

    # Wait until every worker has connected and warmed up.
    ready_workers = set()
    startup_errors = []

    ready_deadline = time.monotonic() + READY_TIMEOUT_SECONDS

    while len(ready_workers) < clients:
        remaining = ready_deadline - time.monotonic()

        if remaining <= 0:
            break

        try:
            message = ready_queue.get(timeout=min(
                1.0, remaining
            ))
        except queue.Empty:
            if not any(p.is_alive() for p in processes):
                break
            continue

        ready_workers.add(message["worker_id"])

        if message["status"] != "ready":
            startup_errors.append(message)

    if len(ready_workers) != clients:
        # Release any workers still waiting, then stop the run.
        start_event.set()

        for process in processes:
            process.join(timeout=5)

        for process in processes:
            if process.is_alive():
                process.terminate()
                process.join()

        raise RuntimeError(
            f"Startup failed for {engine} {workload}; "
            f"ready={len(ready_workers)}/{clients}; "
            f"errors={startup_errors}"
        )

    # Use a common monotonic clock for all processes.
    release = time.perf_counter()

    with release_time.get_lock():
        release_time.value = release

    with deadline_time.get_lock():
        deadline_time.value = release + TEST_SECONDS

    # Release all workers together.
    start_event.set()

    for process in processes:
        process.join(
            timeout=TEST_SECONDS + QUERY_TIMEOUT_SECONDS + 30
        )

    # Stop workers that have hung beyond the safety limit.
    for process in processes:
        if process.is_alive():
            process.terminate()
            process.join()

    # Collect worker output. Workers exit only after placing
    # their records on the queue.
    all_messages = []

    while True:
        try:
            all_messages.append(
                log_queue.get(timeout=2)
            )
        except queue.Empty:
            break

    request_rows = [
        message for message in all_messages
        if message.get("status") in (
            "success", "error", "timeout"
        )
        and "latency_ms" in message
    ]

    worker_finish_times = [
        message["finish_perf"]
        for message in all_messages
        if message.get("status") == "worker_finished"
        and "finish_perf" in message
    ]

    worker_errors = [
        message for message in all_messages
        if message.get("status") == "worker_error"
    ]

    for row in request_rows:
        row.update({
            "clients": clients,
            "repetition": repetition,
        })
        # Do not export the internal monotonic timestamps.
        row.pop("finish_perf", None)
        row.pop("release_perf", None)
        row.pop("deadline_perf", None)

    summary = summarize_run(
        engine,
        workload,
        clients,
        repetition,
        request_rows,
        worker_finish_times,
        release,
    )

    summary["worker_errors"] = len(worker_errors)

    print("Summary:", summary)

    if worker_errors:
        for item in worker_errors:
            print("WORKER ERROR:", item)

    return request_rows, summary


# ============================================================
# MAIN BENCHMARK
# ============================================================

def main():
    queries = load_queries()

    # Validate both database connections before starting.
    print("Checking PostgreSQL connection...")
    with connect_postgres() as conn:
        conn.execute("SELECT 1").fetchone()
    print("PostgreSQL connection successful.")

    print("Checking Neo4j connection...")
    driver = connect_neo4j()
    try:
        with driver.session(
            database=NEO_DATABASE
        ) as session:
            session.run("RETURN 1").consume()
    finally:
        driver.close()

    print("Neo4j connection successful.")
    print("Starting concurrency benchmarks...")

    all_requests = []
    all_summaries = []

    # Q4: reachability workload at depth 10.
    # Q7: shortest directed transfer path, bound 6.
    for engine in ("PostgreSQL", "Neo4j"):
        for workload in ("Q4", "Q7"):
            query = queries[(engine, workload)]

            for clients in CLIENT_COUNTS:
                for repetition in range(
                    1, REPETITIONS + 1
                ):
                    request_rows, summary = run_one(
                        engine,
                        workload,
                        clients,
                        repetition,
                        query,
                    )

                    all_requests.extend(request_rows)
                    all_summaries.append(summary)

                    # Persist results after each repetition
                    # in case a later run fails.
                    write_csv(
                        OUTPUT_DIR / "requests.csv",
                        all_requests,
                    )

                    write_csv(
                        OUTPUT_DIR / "run_summaries.csv",
                        all_summaries,
                    )

    # Aggregate the three repetitions using medians.
    grouped = {}

    for row in all_summaries:
        key = (
            row["engine"],
            row["workload"],
            row["clients"],
        )
        grouped.setdefault(key, []).append(row)

    aggregate_rows = []

    for key, runs in sorted(grouped.items()):
        engine, workload, clients = key

        aggregate_rows.append({
            "engine": engine,
            "workload": workload,
            "clients": clients,
            "repetitions": len(runs),
            "median_throughput_rps": round(
                statistics.median(
                    r["throughput_rps"] for r in runs
                ),
                3,
            ),
            "median_latency_ms": (
                round(
                    statistics.median([
                        r["median_latency_ms"]
                        for r in runs
                        if r["median_latency_ms"] is not None
                    ]),
                    3,
                )
                if any(
                    r["median_latency_ms"] is not None
                    for r in runs
                )
                else None
            ),
            "median_p95_latency_ms": (
                round(
                    statistics.median([
                        r["p95_latency_ms"]
                        for r in runs
                        if r["p95_latency_ms"] is not None
                    ]),
                    3,
                )
                if any(
                    r["p95_latency_ms"] is not None
                    for r in runs
                )
                else None
            ),
            "total_errors": sum(
                r["errors"] for r in runs
            ),
            "total_timeouts": sum(
                r["timeouts"] for r in runs
            ),
            "total_worker_errors": sum(
                r["worker_errors"] for r in runs
            ),
        })

    write_csv(
        OUTPUT_DIR / "aggregate_summary.csv",
        aggregate_rows,
    )

    print("\nBENCHMARK COMPLETED")
    print("Results directory:", OUTPUT_DIR)
    print("Requests: requests.csv")
    print("Per-run summaries: run_summaries.csv")
    print("Aggregate results: aggregate_summary.csv")


if __name__ == "__main__":
    mp.freeze_support()
    main()
