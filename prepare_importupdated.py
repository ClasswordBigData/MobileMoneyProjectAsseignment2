
import csv
import json
import time
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
SOURCE_DIR = BASE_DIR / "csv"
OUTPUT_DIR = BASE_DIR / "neo4j_import"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

EXPECTED = {
    "customers": 228_000,
    "accounts": 248_000,
    "devices": 228_600,
    "owns": 248_000,
    "account_devices": 285_600,
    "transfers": 20_003_300,
}


def convert_file(source_name, output_name, columns, transform):
    source_path = SOURCE_DIR / source_name
    output_path = OUTPUT_DIR / output_name

    if not source_path.is_file():
        raise FileNotFoundError(f"Source CSV not found: {source_path}")

    count = 0
    started = time.perf_counter()

    with source_path.open("r", newline="", encoding="utf-8") as src:
        reader = csv.DictReader(src)

        if not reader.fieldnames:
            raise ValueError(f"CSV has no header: {source_path}")

        with output_path.open("w", newline="", encoding="utf-8") as dst:
            writer = csv.writer(dst, lineterminator="\n")
            writer.writerow(columns)

            for row in reader:
                writer.writerow(transform(row))
                count += 1

                if count % 1_000_000 == 0:
                    print(f"{output_name}: {count:,} rows written")

    elapsed = round(time.perf_counter() - started, 2)
    size_bytes = output_path.stat().st_size

    print(
        f"Created {output_path} | rows={count:,} | "
        f"bytes={size_bytes:,} | seconds={elapsed}"
    )

    return {
        "rows": count,
        "bytes": size_bytes,
        "elapsed_seconds": elapsed,
        "path": str(output_path),
    }


def main():
    results = {}

    # Nodes: each node type has its own ID group and label.
    results["customers"] = convert_file(
        "customers.csv",
        "customers_typed.csv",
        [
            "customer_id:ID(Customer-ID)",
            "region:string",
            ":LABEL",
        ],
        lambda r: [
            r["customer_id"],
            r["region"],
            "Customer",
        ],
    )

    results["accounts"] = convert_file(
        "accounts.csv",
        "accounts_typed.csv",
        [
            "account_id:ID(Account-ID)",
            "role:string",
            ":LABEL",
        ],
        lambda r: [
            r["account_id"],
            r["role"],
            "Account",
        ],
    )

    results["devices"] = convert_file(
        "devices.csv",
        "devices_typed.csv",
        [
            "device_id:ID(Device-ID)",
            "access_type:string",
            ":LABEL",
        ],
        lambda r: [
            r["device_id"],
            r["access_type"],
            "Device",
        ],
    )

    # Customer OWNS Account.
    results["owns"] = convert_file(
        "accounts.csv",
        "owns_typed.csv",
        [
            ":START_ID(Customer-ID)",
            ":END_ID(Account-ID)",
            ":TYPE",
        ],
        lambda r: [
            r["customer_id"],
            r["account_id"],
            "OWNS",
        ],
    )

    # Account USED_DEVICE Device.
    results["account_devices"] = convert_file(
        "account_devices.csv",
        "used_device_typed.csv",
        [
            ":START_ID(Account-ID)",
            ":END_ID(Device-ID)",
            "first_day:int",
            "last_day:int",
            "observations:int",
            ":TYPE",
        ],
        lambda r: [
            r["account_id"],
            r["device_id"],
            r["first_day"],
            r["last_day"],
            r["observations"],
            "USED_DEVICE",
        ],
    )

    # Directed Account TRANSFER Account.
    results["transfers"] = convert_file(
        "transfers.csv",
        "transfers_typed.csv",
        [
            ":START_ID(Account-ID)",
            ":END_ID(Account-ID)",
            "transfer_id:long",
            "occurred_at:long",
            "amount_rwf:long",
            ":TYPE",
        ],
        lambda r: [
            r["sender_id"],
            r["receiver_id"],
            r["transfer_id"],
            r["occurred_at"],
            r["amount_rwf"],
            "TRANSFER",
        ],
    )

    print("\nRow-count validation")
    all_match = True

    for name, expected_count in EXPECTED.items():
        actual_count = results[name]["rows"]
        matches = actual_count == expected_count

        print(
            f"{name}: actual={actual_count:,}, "
            f"expected={expected_count:,}, match={matches}"
        )

        all_match = all_match and matches

    audit = {
        "expected_counts": EXPECTED,
        "results": results,
        "all_counts_match": all_match,
        "generated_at_epoch": int(time.time()),
    }

    audit_path = OUTPUT_DIR / "prepare_import_audit.json"
    audit_path.write_text(
        json.dumps(audit, indent=2),
        encoding="utf-8",
    )

    print(f"\nAudit saved to: {audit_path}")

    if not all_match:
        raise SystemExit(
            "ERROR: At least one converted file has an unexpected row count."
        )

    print("\nSUCCESS: All Neo4j import files passed row-count validation.")
    print("Node labels: Customer, Account, Device")
    print("Relationship types: OWNS, USED_DEVICE, TRANSFER")


if __name__ == "__main__":
    main()
