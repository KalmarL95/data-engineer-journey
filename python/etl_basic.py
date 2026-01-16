with open("data.csv", "r") as file:
    header = next(file)

    total_error_amount = 0

    for line in file:
        line = line.strip()

        if not line:
            continue

        parts = line.split(",")

        if len(parts) !=3:
            continue

        _, status, amount = parts

        if status == "ERROR":
            total_error_amount += int(amount)

print("Total ERROR amount:", total_error_amount)
