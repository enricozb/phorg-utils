#! /usr/bin/python

import os
import sys

import utils.exif as exif
import utils.pipeline as pipeline
import utils.processors as processors
import utils.status as status


def get_paths():
    with open(sys.argv[1]) as media_files:
        return [f.strip() for f in media_files if f.strip()]


def main():
    status_connection = status.Connection("/tmp/phorg_import.sock")
    status_connection.start()

    paths = get_paths()
    dest_dir = sys.argv[2]
    if not os.path.isabs(dest_dir):
        raise ValueError("Destination directory must be absolute")

    status_connection.message("exif: gathering data")
    import_pipeline = pipeline.Pipeline(
        paths=paths,
        results={"exif": exif.exif(paths), "dest_dir": dest_dir},
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

    status_connection.finish(media, import_pipeline.errors)


if __name__ == "__main__":
    main()
