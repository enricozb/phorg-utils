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
    except (AttributeError, TypeError, ValueError):
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
    copy(path, os.path.join(results["media_dir"], filename))
    return filename


def did_convert(path, results):
    return results["convert"].get(path) is not None


def thumb(path, results, max_width=128, max_height=128):
    if not did_convert(path, results):
        return

    path_filetype = results["exif"][path]["File"]["FileType"]

    if path_filetype not in ("JPEG", "PNG"):
        return

    width = exif.search(results["exif"][path], "ImageWidth")
    height = exif.search(results["exif"][path], "ImageHeight")

    # convert myfigure.png -resize 200x100 myfigure.jpg
    if width >= height:
        height = int(round(128 * height / width))
        height += height % 8
        width = 128

    else:
        width = int(round(128 * width / height))
        width += width % 8
        height = 128

    subprocess.run(
        [
            "convert",
            os.path.join(results["media_dir"], results["convert"][path]),
            "-resize",
            f"{width}x{height}",
            os.path.join(results["thumb_dir"], f"{results['guid'][path]}.jpg"),
        ],
        check=True,
        stdout=subprocess.PIPE,
    )


def set_utime(path, results):
    if not did_convert(path, results):
        return
    dest_path = results["convert"][path]

    path_time = results["timestamp"][path]
    dt = datetime.datetime.strptime(path_time, "%Y:%m:%d %H:%M:%S.%f")
    unix_time = time.mktime(dt.timetuple())
    os.utime(path, (unix_time, unix_time))
    os.utime(os.path.join(results["media_dir"], dest_path), (unix_time, unix_time))


def set_guid(path, results):
    if not did_convert(path, results):
        return
    dest_path = results["convert"][path]

    subprocess.run(
        [
            "exiftool",
            "-overwrite_original",
            path,
            os.path.join(results["media_dir"], dest_path),
            f"-ImageUniqueID={results['guid'][path]}",
        ],
        check=True,
        stdout=subprocess.PIPE,
    )
