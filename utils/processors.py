import datetime
import json
import os
import shutil
import subprocess
import time
import uuid

import utils.exif as exif
from utils.error import PhorgError


def kind(path, results):
    path_kind = results["exif"][path]["File"]["MIMEType"].split("/")[0]
    if path_kind not in ("image", "video"):
        raise PhorgError(f"Invalid kind {kind} for {path}")
    return path_kind


def guid(path, results):
    guid = exif.search(results["exif"][path], "ImageUniqueID")

    try:
        guid = guid.lower()
        uuid.UUID(guid, version=4)
        if guid in results["existing_guids"]:
            raise PhorgError(f"Already imported {path}")

        return guid
    except (TypeError, ValueError):
        pass

    guid = str(uuid.uuid4()).lower()

    return guid


def timestamp(path, results):
    all_dates = json.loads(
        subprocess.check_output(["exiftool", "-AllDates", "-j", "-g", path])
    )[0]

    datetimes = []
    for exif_category, exif_data in all_dates.items():
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
                raise PhorgError(f"Unknown time format {time_str} for {path}")

    if not datetimes:
        # file modification date is 'YYYY:MM:DD HH:MM:SSpZZ:ZZ', which has length 25
        modify_date = results["exif"][path]["File"]["FileModifyDate"]
        if len(modify_date) != 25:
            raise PhorgError(f"Bad FileModifyDate {modify_date} for {path}")

        return modify_date[:-6] + ".000000"

    min_datetime = min(datetimes)
    microsecond = min_datetime.microsecond
    if "EXIF" in results["exif"][path]:
        microsecond = int(
            results["exif"][path]["EXIF"].get("SubSecTimeOriginal", microsecond)
        )

    microsecond = str(microsecond).ljust(6, "0")

    if len(microsecond) != 6:
        raise PhorgError("Long microsecond precision of {microsecond} in {path}")

    timestamp_no_us = min_datetime.replace(microsecond=0).strftime("%Y:%m:%d %H:%M:%S")
    return f"{timestamp_no_us}.{microsecond}"


def burst_id(path, results):
    return exif.search(results["exif"][path], "BurstUUID")


def content_id(path, results):
    return exif.search(results["exif"][path], "ContentIdentifier")


def dupe_guid(results, errors):
    guid_to_paths = {}
    for path, path_guid in results["guid"].items():
        if path_guid is None:
            continue

        guid_to_paths.setdefault(path_guid, []).append(path)

    for path_guid, paths in guid_to_paths.items():
        if len(paths) > 1:
            errors.append(f"Duplicate guids({path_guid}): {paths}")
            for path in paths:
                results["guid"][path] = None


def convert(
    path,
    results,
    formats={
        "PNG": "png",
        "JPEG": "jpg",
        "MP4": "mp4",
    },
):
    def copy(src, dst):
        shutil.copyfile(src, dst, follow_symlinks=False)
        shutil.copymode(src, dst, follow_symlinks=False)
        shutil.copystat(src, dst, follow_symlinks=False)

    # final checks whether or not this file should be converted
    path_guid = results["guid"][path]
    path_kind = results["kind"][path]
    path_time = results["timestamp"][path]

    if None in (path_guid, path_kind, path_time):
        # no error because each of guid/kind/time already produce errors
        return

    path_filetype = results["exif"][path]["File"]["FileType"]
    if path_filetype not in formats:
        return Error(f"Unknown input format {path_filetype} for file {path}")

    filename = f"{path_time}::{path_guid}.{formats[path_filetype]}"
    copy(path, os.path.join(results["dest_dir"], filename))
    return filename


def thumb(path, results):
    pass


def set_utime(path, results):
    dest_path = results["convert"].get(path)
    if dest_path is None:
        return

    path_time = results["timestamp"][path]
    dt = datetime.datetime.strptime(path_time, "%Y:%m:%d %H:%M:%S.%f")
    unix_time = time.mktime(dt.timetuple())
    os.utime(path, (unix_time, unix_time))
    os.utime(os.path.join(results["dest_dir"], dest_path), (unix_time, unix_time))


def set_guid(path, results):
    dest_path = results["convert"].get(path)
    if dest_path is None:
        return

    subprocess.run(
        [
            "exiftool",
            "-overwrite_original",
            path,
            os.path.join(results["dest_dir"], dest_path),
            f"-ImageUniqueID={results['guid'][path]}",
        ],
        check=True,
        stdout=subprocess.PIPE,
    )
