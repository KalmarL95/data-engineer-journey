import csv
import sys
import logging
from typing import Dict, Tuple

VALID_STATUSES = {"OK", "ERROR"}


def setup_logger() -> None:
    #basicConfig set the log format and level
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )


def validate_row(row: Dict[str, str]) -> Tuple[bool, str]:
    #it show that the row is valid or not(in that case + reason)
    if not row.get("id"):
        return False, "missing_id"
    if row.get("status") not in VALID_STATUSES:
        return False, "invalid_status"
    if not row.get("amount"):
        return False, "missing_amount"
    return True, "ok"


def write_metrics(total: int, valid: int, invalid: int) -> None:
    with open("output/metrics.txt", "w") as metrics:
        metrics.write(f"total={total}\n")
        metrics.write(f"valid={valid}\n")
        metrics.write(f"invalid={invalid}\n")


def main() -> int:
    if len(sys.argv) < 2:
        print("Usage: python3 sec/ref_etl_pipeline.py <input_csv>")
        return 1

    input_file = sys.argv[1]

    total = 0
    valid = 0
    invalid = 0

    setup_logger()
    logging.info("Starting ETL. input_file=%s", input_file)

    with open(input_file, newline="") as infile, \
         open("output/invalid_rows.csv", "w", newline="") as invalid_file, \
         open("output/valid_rows.csv", "w", newline="") as valid_file:
            reader = csv.DictReader(infile)
            invalid_writer = csv.DictWriter(invalid_file, fieldnames=reader.fieldnames)
            valid_writer = csv.DictWriter(valid_file, fieldnames=reader.fieldnames)
            invalid_writer.writeheader()
            valid_writer.writeheader()

            for row in reader:
                total += 1
                is_valid, reason = validate_row(row)

                if not is_valid:
                    invalid_writer.writerow(row)
                    invalid += 1
                    logging.warning("Invalid row (%s): %s", reason, row)
                    continue

                valid_writer.writerow(row)
                valid += 1

    write_metrics(total, valid, invalid)

    logging.info("Finished ETL. total=%d valid=%d invalid=%d", total, valid, invalid)

    if invalid > 0:
        return 2
    return 0



if __name__ == "__main__":
    raise SystemExit(main())

