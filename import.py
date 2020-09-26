#! /usr/bin/python

import dataclasses
import datetime
import json
import multiprocessing
import os
import shutil
import socket
import subprocess
import sys
import time
import uuid

SOCKET = None
CONFIG_DIR = os.environ.get(
    "XDG_CONFIG_HOME", os.path.join(os.environ["HOME"], ".config/")
)
IMPORT_STATUS_PATH = os.path.join(CONFIG_DIR, "phorg/import_status.json")
ERRORS = []


@dataclasses.dataclass
class Error:
    error_message: str


def connect_socket():
    global SOCKET
    SOCKET = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    SOCKET.connect("/tmp/phorg_import.sock")


def write_status(ongoing, percentage, message, media, complete=False, errors=False):
    SOCKET.send(
        json.dumps(
            {
                "ongoing": ongoing,
                "complete": complete,
                "percentage": percentage,
                "message": message,
                "errors": ERRORS if errors else [],
                "media": media,
            },
        ).encode()
    )
    SOCKET.send(b"\n")


def copy(src, dst):
    return
    shutil.copyfile(src, dst, follow_symlinks=False)
    shutil.copymode(src, dst, follow_symlinks=False)
    shutil.copystat(src, dst, follow_symlinks=False)


def get_files():
    with open(sys.argv[1]) as media_files:
        return [f.strip() for f in media_files if f.strip()]


LAST_UPDATE = 0
UPDATE_RATE = 1


def update_progress(message, percentage=0, step=1, of=1, force=False):
    global LAST_UPDATE

    now = time.time()
    if force or now - LAST_UPDATE > UPDATE_RATE:
        LAST_UPDATE = now
        write_status(
            ongoing=True,
            percentage=(percentage + step - 1) / of,
            message=message,
            media=[],
        )


# ----- parallel functions -----
# the accumulator of the parallel computations
info = {}


def multi_do(func, files, *, step, of, processes=16):
    num_files = len(files)
    start = time.time()
    info[func.__name__] = {}

    with multiprocessing.Pool(processes) as p:
        for (i, f), result in zip(enumerate(files), p.imap(func, files, processes)):
            if type(result) is Error:
                ERRORS.append(result.error_message)
                info[func.__name__][f] = None
            else:
                info[func.__name__][f] = result

            update_progress(
                f"{func.__name__}: {i} of {num_files}", i / num_files, step, of
            )

    print(f"DONE: {func.__name__}, {time.time() - start: 0.2f} sec")
    update_progress(
        f"{func.__name__}: {num_files} of {num_files}", 1, step, of, force=True
    )


def check_dupe_guids():
    info["guid_dupe"] = {}
    for path, path_guid in info["guid"].items():
        info["guid_dupe"][path_guid] = path_guid in info["guid_dupe"]
        if info["guid_dupe"][path_guid]:
            ERRORS.append(f"Duplicate guid {path}")


search_memo = {}


def search(exif_data, key):
    for exif_category in search_memo.setdefault(key, []):
        if exif_category in exif_data and key in exif_data[exif_category]:
            return exif_data[exif_category][key]

    for exif_category, data in exif_data.items():
        if exif_category == "SourceFile":
            continue

        if key in data:
            search_memo[key].append(exif_category)
            return data[key]


def exif(files):
    start = time.time()
    exiftool_output = json.loads(
        subprocess.check_output(
            ["exiftool", "-j", "-g", "-@", "-"], input="\n".join(files).encode()
        )
    )
    info["exif"] = {}
    for entry in exiftool_output:
        info["exif"][entry["SourceFile"]] = entry
    print(f"DONE: exif, {time.time() - start: 0.2f} sec")


def kind(path):
    return info["exif"][path]["File"]["MIMEType"].split("/")[0]


def guid(path):
    guid = search(info["exif"][path], "ImageUniqueID")

    # if the guid is valid then return it
    if guid is not None:
        try:
            uuid.UUID(guid)
            return guid.lower()
        except ValueError:
            pass

    guid = str(uuid.uuid4()).lower()
    subprocess.run(
        ["exiftool", path, f"-ImageUniqueID={guid}"],
        check=True,
        stdout=subprocess.PIPE,
    )

    return guid.lower()


def timestamp(path):
    times = json.loads(
        subprocess.check_output(["exiftool", "-AllDates", "-j", "-g", path])
    )[0]

    datetimes = []
    for exif_category, exif_data in times.items():
        if type(exif_data) is str:
            continue

        for exif_tag, time_str in exif_data.items():
            if time_str == "0000:00:00 00:00:00":
                continue
            for format in ["%Y:%m:%d %H:%M:%S", "%Y:%m:%d %H:%M:%S.%f"]:
                try:
                    datetimes.append(datetime.datetime.strptime(time_str, format))
                    break
                except ValueError:
                    pass
            else:
                return Error(f"Failed to parse {time_str} for {path}")

    if not datetimes:
        # file modification date is 'YYYY:MM:DD HH:MM:SSpZZ:ZZ', which has length 26
        modify_date = info["exif"][path]["File"]["FileModifyDate"]
        if len(modify_date) != 25:
            return Error(f"Bad FileModifyDate {modify_date} for {path}")

        return modify_date[:-6] + ".000000"

    min_datetime = min(datetimes)
    microsecond = min_datetime.microsecond
    if "EXIF" in info["exif"][path]:
        microsecond = int(
            info["exif"][path]["EXIF"].get("SubSecTimeOriginal", microsecond)
        )

    microsecond = str(microsecond).ljust(6, "0")

    if len(microsecond) != 6:
        return Error("Long microsecond precision of {microsecond} in {path}")

    timestamp_no_us = min_datetime.replace(microsecond=0).strftime("%Y:%m:%d %H:%M:%S")
    return f"{timestamp_no_us}.{microsecond}"


def burst_id(path):
    return search(info["exif"][path], "BurstUUID")


def content_id(path):
    return search(info["exif"][path], "ContentIdentifier")


# source and destination file formats
CONVERT_FORMATS = {
    "PNG": "png",
    "JPEG": "jpg",
    "MP4": "mp4",
}


def convert(path):
    if not should_import(path):
        return

    filetype = info["exif"][path]["File"]["FileType"]
    guid = info["guid"][path]
    timestamp = info["timestamp"][path]

    if filetype not in CONVERT_FORMATS:
        return Error(f"Unknown input format {filetype} for file {path}")

    filename = f"{timestamp}::{guid}.{CONVERT_FORMATS[filetype]}"
    copy(path, os.path.join(info["destdir"], filename))
    return filename


def thumb(path):
    pass


def should_import(src):
    src_guid = info["guid"][src]
    src_kind = info["kind"][src]
    src_timestamp = info["timestamp"][src]

    if None in (src_guid, src_kind, src_timestamp) or info["guid_dupe"][src_guid]:
        return False

    return True


def set_utime(src, timestamp_str):
    datetime_obj = datetime.datetime.strptime(timestamp_str, "%Y:%m:%d %H:%M:%S.%f")
    unix_time = time.mktime(datetime_obj.timetuple())
    os.utime(src, (unix_time, unix_time))


def main():
    connect_socket()
    write_status(ongoing=True, percentage=0, message="", media=[])

    files = get_files()
    if not os.path.isabs(sys.argv[2]):
        raise ValueError("Destination directory must be absolute")

    info["destdir"] = sys.argv[2]

    print("importing to", info["destdir"])

    update_progress(f"exif: gathering data")
    exif(files)

    multi_do(timestamp, files, step=1, of=7)
    multi_do(kind, files, step=2, of=7)
    multi_do(guid, files, step=3, of=7)
    multi_do(burst_id, files, step=4, of=7)
    multi_do(content_id, files, step=5, of=7)

    check_dupe_guids()

    multi_do(convert, files, processes=1, step=6, of=7)
    multi_do(thumb, files, processes=8, step=7, of=7)

    media = {
        "items": {},
        "burst_id": {},
        "content_id": {},
    }

    for src, dst in info["convert"].items():
        if dst is None or not should_import(src):
            continue

        src_guid = info["guid"][src]
        src_kind = info["kind"][src]
        src_timestamp = info["timestamp"][src]
        src_burst_id = info["burst_id"][src]
        src_content_id = info["content_id"][src]

        if src_guid in media["items"]:
            print(src, media["items"][src_guid]["original"])
            assert False

        media["items"][src_guid] = {
            "original": src,
            "filename": dst,
            "timestamp": src_timestamp,
            "burst_id": src_burst_id,
            "content_id": src_content_id,
        }

        if src_burst_id:
            media["burst_id"].setdefault(src_burst_id, []).append(src_guid)

        if src_content_id:
            media["content_id"].setdefault(src_content_id, {})[src_kind] = src_guid

        set_utime(src, src_timestamp)

    write_status(
        ongoing=False,
        percentage=1,
        message="Done!",
        media=media,
        complete=True,
        errors=True,
    )

    SOCKET.close()


if __name__ == "__main__":
    main()
