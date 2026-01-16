import sys

if len(sys.argv) < 2:
    print("Usage: python3 etl_param.py <input_file>")
    sys.exit[1]

filename = sys.argv[1]

with open(filename, "r") as infile, open("errors.txt", "w") as outfile:
    next(infile)

    for line in infile:
        if "ERROR" in line:
            outfile.write(line)

