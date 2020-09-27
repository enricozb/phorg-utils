import json
import subprocess

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
    exiftool_output = json.loads(
        subprocess.check_output(
            ["exiftool", "-j", "-g", "-@", "-"], input="\n".join(files).encode()
        )
    )
    return {entry["SourceFile"]: entry for entry in exiftool_output}
