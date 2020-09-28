#! /usr/bin/python

import json
import os
import sys

import utils.exif as exif
import utils.pipeline as pipeline
import utils.processors as processors
import utils.status as status


def get_paths(paths_file):
    with open(paths_file) as media_files:
        return [f.strip() for f in media_files if f.strip()]


def get_existing_guids(lib_path):
    with open(os.path.join(lib_path, "phorg-lib.json")) as lib_file:
        return set(json.load(lib_file)["media"]["items"].keys())


def parse_library_path(lib_path):
    """
    Returns the set of existing media guids and the destination directory for media.
    """
    return (
        get_existing_guids(lib_path),
        os.path.join(lib_path, "media"),
        os.path.join(lib_path, "thumb"),
    )


def main():
    if len(sys.argv) != 3:
        print("Incorrect number of arguments")
        exit(1)

    status_connection = status.Connection("/tmp/phorg_import.sock")
    status_connection.start()

    print("running with:", sys.argv)

    if not os.path.isabs(sys.argv[2]):
        raise ValueError("Library path must be absolute")

    paths = get_paths(sys.argv[1])
    existing_guids, media_dir, thumb_dir = parse_library_path(sys.argv[2])

    status_connection.message("exif: gathering data")
    import_pipeline = pipeline.Pipeline(
        paths=paths,
        results={
            "exif": exif.exif(paths),
            "existing_guids": existing_guids,
            "media_dir": media_dir,
            "thumb_dir": thumb_dir,
        },
        procs=16,
        progress_callback=status_connection.progress,
    )

    import_pipeline.par_do(processors.timestamp)
    import_pipeline.par_do(processors.kind)
    import_pipeline.par_do(processors.guid)
    import_pipeline.par_do(processors.burst_id)
    import_pipeline.par_do(processors.content_id)
    import_pipeline.results_do(processors.dupe_guid)
    import_pipeline.par_do(processors.convert, procs=1)
    import_pipeline.par_do(processors.thumb, procs=1)
    import_pipeline.par_do(processors.set_guid, procs=8)
    import_pipeline.par_do(processors.set_utime)

    import_pipeline.run()

    results = import_pipeline.results

    media = {
        "items": {},
        "burst_id": {},
        "content_id": {},
    }

    for src, dst in results["convert"].items():
        if dst is None:
            continue

        src_guid = results["guid"][src]
        src_kind = results["kind"][src]
        src_timestamp = results["timestamp"][src]
        src_burst_id = results["burst_id"][src]
        src_content_id = results["content_id"][src]

        if src_guid in media["items"]:
            print(src, media["items"][src_guid]["original"])
            assert False

        media["items"][src_guid] = {
            "filename": dst,
            "timestamp": src_timestamp,
            "burst_id": src_burst_id,
            "content_id": src_content_id,
        }

        if src_burst_id:
            media["burst_id"].setdefault(src_burst_id, []).append(src_guid)

        if src_content_id:
            media["content_id"].setdefault(src_content_id, {})[src_kind] = src_guid

    status_connection.finish(media, import_pipeline.errors)


if __name__ == "__main__":
    main()
