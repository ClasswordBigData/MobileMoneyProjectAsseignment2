
import csv
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
SOURCE_DIR = BASE_DIR / "csv"
OUTPUT_DIR = BASE_DIR / "neo4j_import"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def convert_file(source_name, output_name, columns, transform):
    source_path = SOURCE_DIR / source_name
    output_path = OUTPUT_DIR / output_name

    count = 0
    with source_path.open("r", newline="", encoding="utf-8") as src:
        reader = csv.DictReader(src)

        with output_path.open("w", newline="", encoding="utf-8") as dst:
            writer = csv.writer(dst)
            writer.writerow(columns)

            for row in reader:
                writer.writerow(transform(row))
                count += 1

                if count % 1_000_000 == 0:
                    print(f"{output_name}: {count:,} rows")

    print(f"Created {output_path} ({count:,} rows)")
    return count


def main():
    counts = {}

    # Node files: customer, account, and device IDs have separate ID groups.
    counts["customers"] = convert_file(
        "customers.csv",
        "customers_typed.csv",
        ["customer_id:ID(Customer-ID)", "region:string"],
        lambda r: [r["customer_id"], r["region"]],
    )

    counts["accounts"] = convert_file(
        "accounts.csv",
        "accounts_typed.csv",
        ["account_id:ID(Account-ID)", "role:string"],
        lambda r: [r["account_id"], r["role"]],
    )

    counts["devices"] = convert_file(
        "devices.csv",
        "devices_typed.csv",
        ["device_id:ID(Device-ID)", "access_type:string"],
        lambda r: [r["device_id"], r["access_type"]],
    )

    # Customer OWNS Account.
    counts["owns"] = convert_file(
        "accounts.csv",
        "owns_typed.csv",
        [":START_ID(Customer-ID)", ":END_ID(Account-ID)", ":TYPE"],
        lambda r: [r["customer_id"], r["account_id"], "OWNS"],
    )

    # Account USED_DEVICE Device.
    counts["account_devices"] = convert_file(
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
    counts["transfers"] = convert_file(
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

    expected = {
        "customers": 228_000,
        "accounts": 248_000,
        "devices": 228_600,
        "owns": 248_000,
        "account_devices": 285_600,
        "transfers": 20_003_300,
    }

    print("\nValidation:")
    all_match = True
    for name, expected_count in expected.items():
        actual = counts[name]
        ok = actual == expected_count
        print(
            f"{name}: actual={actual:,}, "
            f"expected={expected_count:,}, match={ok}"
        )
        all_match = all_match and ok

    if not all_match:
        raise SystemExit("ERROR: A converted file has an unexpected row count.")

    print("\nAll Neo4j import file counts match.")


if __name__ == "__main__":
    main()
