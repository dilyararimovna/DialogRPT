import bz2, json, os, pickle, pdb, time, random
import urllib.request
import numpy as np
from shared import _cat_


def extract_rc(date):
    path_bz2 = '%s/RC_%s.bz2' % (fld_bz2, date)
    nodes = dict()
    edges = dict()
    subs = set()
    n = 0
    m = 0
    kk = ['body', 'link_id', 'name', 'parent_id', 'subreddit']

    def save(nodes, edges):
        for sub in nodes:
            fld = fld_jsonl + '/' + sub
            try:
                os.makedirs(fld, exist_ok=True)
            except NotADirectoryError as e:
                print(e)
                continue
            if sub not in subs:
                open(fld + '/%s_nodes.jsonl' % date, 'w', encoding="utf-8")
                open(fld + '/%s_edges.tsv' % date, 'w', encoding="utf-8")
                subs.add(sub)
            with open(fld + '/%s_nodes.jsonl' % date, 'a', encoding="utf-8") as f:
                f.write('\n'.join(nodes[sub]) + '\n')
            with open(fld + '/%s_edges.tsv' % date, 'a', encoding="utf-8") as f:
                f.write('\n'.join(edges[sub]) + '\n')

    for line in bz2.open(path_bz2, 'rt', encoding="utf-8"):
        n += 1
        line = line.strip('\n')
        try:
            node = json.loads(line)
            print(json.dumps(node, indent=4))
        except Exception:
            continue


def extract_rs(date):
    import zstd
    path_bz2 = '%s/RS_%s.zst' % (fld_bz2, date)
    roots = dict()
    subs = set()
    n = 0
    m = 0
    kk = ['selftext', 'id', 'title', 'subreddit']

    def save(roots):
        for sub in roots:
            fld = fld_jsonl + '/' + sub
            try:
                os.makedirs(fld, exist_ok=True)
            except NotADirectoryError as e:
                print(e)
                continue
            if sub not in subs:
                open(fld + '/%s_roots.jsonl' % date, 'w', encoding="utf-8")
                subs.add(sub)
            with open(fld + '/%s_roots.jsonl' % date, 'a', encoding="utf-8") as f:
                f.write('\n'.join(roots[sub]) + '\n')

    with open(path_bz2, 'rb') as fh:
        dctx = zstd.ZstdDecompressor(max_window_size=2147483648)
        with dctx.stream_reader(fh) as reader:
            previous_line = ""
            while True:
                chunk = reader.read(2 ** 24)  # 16mb chunks
                if not chunk:
                    break

                string_data = chunk.decode('utf-8')
                lines = string_data.split("\n")
                for i, line in enumerate(lines[:-1]):
                    if i == 0:
                        line = previous_line + line

                    try:
                        root = json.loads(line)
                        print(json.dumps(root, indent=4))
                    except Exception:
                        continue


for date in get_dates(2011):
    extract_rc(date)
    extract_rs(date)
