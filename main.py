from datetime import timedelta
from typing import List
import zlib

import zstd
from takethetime import ttt

import aw_client
from aw_core.models import Event


def chunked(l, chunk_size):
    for i in range(0, len(l), chunk_size):
        yield l[i:i + chunk_size]


def chunked_by_date(events):
    events_on_date = []
    next_date = events[-1].timestamp.date() + timedelta(days=1)
    for e in sorted(events, reverse=False, key=lambda e: e.timestamp):
        # print(e.timestamp.date(), next_date)
        if e.timestamp.date() < next_date:
            events_on_date.append(e)
        else:
            yield events_on_date
            # print(next_date)
            next_date = e.timestamp.date() + timedelta(days=1)
            events_on_date = [e]
    yield events_on_date


def main():
    api = aw_client.ActivityWatchClient("aw-syncserver")
    buckets = api.get_buckets()

    for bucket_id in buckets:
        events = api.get_events(bucket_id, limit=-1)
        print("bucket: {}\nevent count: {}".format(bucket_id, len(events)))

        print("# Unfiltered")
        compress(events, method="zstd")
        compress(events, method="zlib")

        if False:
            print("# Filtered")
            compress(filter_short(events))

        print("# Chunked (n=1000)")
        bench_chunks(list(chunked(events, 1000)), "zstd")

        print("# Chunked (by date)")
        bench_chunks(list(chunked_by_date(events)), "zstd")

        print("=" * 20)


def bench_chunks(chunks: List[List[Event]], method):
    total_size = 0
    for chunk in chunks:
        total_size += len(compress(chunk, method, stats=False))
    print("number of chunks: {}".format(len(chunks)))
    print("total size of all chunks: {}".format(total_size))


def filter_short(events, stats=True) -> List[Event]:
    filtered = [e for e in events if e.duration > timedelta(seconds=0)]

    if stats:
        percent_filtered = 100 * (1 - len(filtered) / len(events))
        print("filtered: {} ({:f}%)".format(len(events) - len(filtered), percent_filtered))

    return filtered


def print_ratio(before, after):
    size_before = len(before)
    size_after = len(after)
    ratio = size_before / size_after
    print("before:\t{} bytes".format(size_before))
    print("after: \t{} bytes".format(size_after))
    print("ratio: {:f}".format(ratio))


def compress(events, method, stats=True) -> bytes:
    events_json_str = str([e.to_json_dict() for e in events])
    events_json_bytes = bytes(events_json_str, "utf8")

    with ttt(echo=False) as t:
        if method == "zstd":
            cctx = zstd.ZstdCompressor(level=10)
            compressed = cctx.compress(events_json_bytes)
        elif method == "zlib":
            compressed = zlib.compress(events_json_bytes, level=9)
        else:
            raise Exception("invalid method")

    if stats:
        print("time to compress: {}".format(t.duration))
        print_ratio(events_json_bytes, compressed)

    return compressed


if __name__ == "__main__":
    main()
