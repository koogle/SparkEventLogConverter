#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Convert spark event log JSON to CSV
# @ Jakob Frick

from sets import Set
import argparse
import json


def get_headers_from_json(event, headers, prefix=""):
    for key, value in event.items():
        if prefix + key not in headers:
            headers.append(prefix + key)
        if isinstance(value, dict):
            get_headers_from_json(value, headers, prefix + key + ".")
        elif isinstance(value, list):
            for idx, item in enumerate(value):
                if isinstance(item, dict):
                    get_headers_from_json(item, headers,
                                          prefix + key + "." + str(idx) + ".")


def get_headers(file):
    headers = []
    for line in file:
        event = json.loads(line)
        get_headers_from_json(event, headers)
    return headers


def read_values_from_json(event, headers, values, prefix=""):
    for key, value in event.items():
        if isinstance(value, dict):
            read_values_from_json(value, headers, values, prefix + key + ".")
        else:
            values[headers.index(prefix + key)] = str(value)


def get_csv_from_json(event, headers):
    values = [""] * len(headers)
    read_values_from_json(event, headers, values)
    return ",".join(values)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("filename", help="the filename of the Spark eventlog",
                        type=str)
    args = parser.parse_args()

    if args.filename.rfind(".json") != len(args.filename) - len(".json"):
        outName = args.filename + ".json"
    else:
        outName = args.filename[:len(args.filename) - len(".json")] + ".csv"

    outfile = open(outName, "w")

    with open(args.filename) as file:
        headers = get_headers(file)
        file.seek(0)
        outfile.write(",".join(headers))

        for line in file:
            event = json.loads(line)
            csvLine = get_csv_from_json(event, headers)
            outfile.write(csvLine + "\n")
    outfile.close()

if __name__ == "__main__":
    main()
