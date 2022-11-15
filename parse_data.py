import json
import os
import io
import pathlib
import pickle
import re

import zstandard
from joblib import Parallel, delayed

regexp = re.compile("/r/(.*)/comments/(.*)/(.*)/")


def parse_one_line(line):
    try:
        doc = json.loads(line)
    except Exception:
        return ("[deleted]", "[deleted]", False, "[deleted]", "", True, 0, 0)
    subreddit = doc.get("subreddit", "[deleted]")
    author = doc.get("author", "[deleted]")
    link = doc.get("permalink", "[deleted]")
    title = doc.get("title", "[deleted]")
    # text = doc.get("selftext", "")
    created_utc = doc.get("created_utc", 0)
    # over_18 = doc.get("over_18", False)
    is_self = doc.get("is_self", False)
    # score = doc.get("score", 0)

    result = regexp.search(link)
    if result:
        if subreddit == "[deleted]":
            subreddit = result.group(1)
        if title == "[deleted]":
            title = " ".join(result.group(3).split("_"))
    # return (subreddit, author, is_self, title, text, over_18, created_utc, score)
    return (subreddit, author, is_self, created_utc)


def parse_one_chunk(chunk, save):
    last_elem = chunk.rfind(b'{"all_awardings"')
    real_elems = chunk[:last_elem].decode("utf-8").split("\n")
    real_elems[0] = "".join([save[0], real_elems[0]])
    save[0] = chunk[last_elem:].decode("utf-8")
    return real_elems[:-1]


def decompress_zstandard(input_file, output_file):
    input_file = pathlib.Path(input_file)
    save = [""]
    sum_read = 0
    decomp = zstandard.ZstdDecompressor(max_window_size=2147483648)
    with open(input_file, "rb") as compressed, open(output_file, "wb") as destination:
        decomp_stream = decomp.stream_reader(compressed)
        while True:
            chunk = decomp_stream.read(1024 ** 3)
            if not chunk:
                break
            lines = parse_one_chunk(chunk, save)
            sum_read += len(lines)
            # print("\r", sum_read)
            # print(lines[-2])
            # print(lines[-1], "\n")
            out = Parallel(n_jobs=8)(
                delayed(parse_one_line)(line) for line in lines
            )
            pickle.dump(out, destination)


zst_files = [
    "./RS_2020-12.zst"
]

for zst_file in zst_files:
    print(f"start parsing {zst_file}")
    meta_file = zst_file[:-4] + ".pickle"
    decompress_zstandard(zst_file, meta_file)
