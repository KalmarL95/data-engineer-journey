import sys

filename = sys.argv[1]

with open(filename, "r") as file:
    next(file)

    error_count = 0

    for line in file:
        line = line.strip()

        if not line:
            continue

        parts = line.split(",")

        if len(parts) != 3:
            print("SKIPPED BAD LINE:", parts)
            continue

        _, status, _ = parts

        if status == "ERROR":
            error_count += 1

print("ERROR rows:", error_count)
