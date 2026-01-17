import csv

valid_statuses = {"OK", "ERROR"}

with open("data/input.csv", newline="") as infile:
    reader = csv.DictReader(infile)

    for row in reader:
        if not row["id"]:
            print("Missing id:", row)
            continue

        if row["status"] not in valid_statuses:
            print("Invalid status:", row)
            continue
        if not row["amount"]:
            print("Missing amount:", row)
            continue

        print("VALID:", row)
