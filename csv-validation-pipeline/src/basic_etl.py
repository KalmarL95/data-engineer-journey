with open("data/input.csv", "r") as file:
    next(file)

    for line in file:
        parts = line.strip().split(",")
        print(parts)

