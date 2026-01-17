import csv
import sys

if len(sys.argv) < 2:
    print("Usage: python3 etl_pipeline.py <input_csv>")
    sys.exit(1)

input_file = sys.argv[1]

total = 0
valid = 0
invalid = 0

valid_statuses = {"OK", "ERROR"}

with open(input_file, newline="") as infile, \
     open("output/invalid_rows.csv", "w", newline="") as badfile:

    reader = csv.DictReader(infile)
    writer = csv.DictWriter(badfile, fieldnames=reader.fieldnames)
    writer.writeheader()

    for row in reader:
        total += 1

        if not row["id"] or \
            row["status"] not in valid_statuses or \
            not row["amount"]:
            writer.writerow(row)
            invalid += 1
            continue

        valid += 1

print("Total rows:", total)
print("Valid rows:", valid)
print("Invalid rows:", invalid)

with open("output/metrics.txt", "w") as metrics:
    metrics.write(f"total={total}\n")
    metrics.write(f"valid={valid}\n")
    metrics.write(f"invalid={invalid}\n")

if invalid > 0:
    sys.exit(2)

