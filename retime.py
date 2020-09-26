#! /usr/bin/python

import datetime
import os
import pathlib
import sys
import time


def get_files():
    with open(sys.argv[1]) as files:
        return [pathlib.Path(f.strip()) for f in files if f.strip()]


def main():
    for f in get_files():
        time_str = f.with_suffix("").name
        for format in ["%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f"]:
            try:
                dt = datetime.datetime.strptime(time_str, format)
                unix_time = time.mktime(dt.timetuple())
                os.utime(f, (unix_time, unix_time))
                break
            except ValueError:
                pass
        else:
            print(f"Couldn't parse time for {f}")


if __name__ == "__main__":
    main()
