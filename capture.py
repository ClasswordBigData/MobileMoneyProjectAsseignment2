
import csv
import json
import time
import itertools
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo

import mobile_money_generator as g

BASE_DIR = Path(__file__).resolve().parent
CSV_DIR = BASE_DIR / "csv"
LOG_DIR = BASE_DIR / "logs"

CSV_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR.mkdir(parents=True, exist_ok=True)

EXPECTED = {
    "customers": 228_000,
    "accounts": 248_000,
    "devices": 228_600,
    "account_devices": 285_600,
    "transfers": 20_003_300,
}

STREAMS = {
    "customers": g.iter_customers,
    "accounts": g.iter_accounts,
    "devices": g.iter_devices,
    "account_devices": g.iter_account_devices,
    "transfers": g.iter_transfers,
}


def capture_dataset(name, iterator_function):
    output_file = CSV_DIR / f"{name}.csv"
    started = time.perf_counter()
    count = 0
    total_amount = 0
    earliest = None
    latest = None

    # Preview without storing the full dataset in memory.
    preview = list(itertools.islice(iterator_function(), 10))

    if not preview:
        raise RuntimeError(f"{name}: generator returned no preview rows")

    fieldnames = list(preview[0].keys())

    print(f"\n{name}: first {len(preview)} sample rows")
    for row in preview[:3]:
        print(row)

    # Restart the deterministic iterator for the full capture.
    with output_file.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=fieldnames,
            extrasaction="raise"
        )
        writer.writeheader()

        for row in iterator_function():
            writer.writerow(row)
            count += 1

            if name == "transfers":
                timestamp = int(row["occurred_at"])
                amount = int(row["amount_rwf"])

                total_amount += amount
                earliest = timestamp if earliest is None else min(earliest, timestamp)
                latest = timestamp if latest is None else max(latest, timestamp)

                if count % 1_000_000 == 0:
                    print(f"Transfers captured: {count:,}")

    elapsed = round(time.perf_counter() - started, 2)
    size_bytes = output_file.stat().st_size

    result = {
        "dataset": name,
        "expected_rows": EXPECTED[name],
        "actual_rows": count,
        "count_matches": count == EXPECTED[name],
        "csv_file": str(output_file),
        "csv_size_bytes": size_bytes,
        "elapsed_seconds": elapsed,
    }

    if name == "transfers":
        result["total_amount_rwf"] = total_amount
        result["earliest_epoch"] = earliest
        result["latest_epoch"] = latest

        if earliest is not None:
            result["earliest_cat"] = datetime.fromtimestamp(
                earliest, ZoneInfo("Africa/Kigali")
            ).isoformat()

        if latest is not None:
            result["latest_cat"] = datetime.fromtimestamp(
                latest, ZoneInfo("Africa/Kigali")
            ).isoformat()

    print(json.dumps(result, indent=2))
    return result


def main():
    print("Mobile Money Dataset Capture")
    print("Generator metadata:")
    print(g.dataset_info())

    audit = {
        "started_at": datetime.now(
            ZoneInfo("Africa/Kigali")
        ).isoformat(),
        "datasets": [],
    }

    for name, iterator_function in STREAMS.items():
        result = capture_dataset(name, iterator_function)
        audit["datasets"].append(result)

    audit["finished_at"] = datetime.now(
        ZoneInfo("Africa/Kigali")
    ).isoformat()

    audit["all_counts_match"] = all(
        item["count_matches"] for item in audit["datasets"]
    )

    audit_file = LOG_DIR / "capture_audit.json"
    audit_file.write_text(
        json.dumps(audit, indent=2),
        encoding="utf-8"
    )

    print("\nCapture completed.")
    print(f"Audit file: {audit_file}")
    print(f"All expected counts match: {audit['all_counts_match']}")

    if not audit["all_counts_match"]:
        raise SystemExit(
            "ERROR: One or more CSV row counts differ from expected counts."
        )


if __name__ == "__main__":
    main()
