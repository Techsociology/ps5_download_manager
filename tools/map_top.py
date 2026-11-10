#!/usr/bin/env python3
"""map_top.py <linker.map> [N]  - which libraries / objects take the space in the ELF (lld map file, make size)."""
import re, sys, collections
path, top = sys.argv[1], int(sys.argv[2]) if len(sys.argv) > 2 else 20
lib, obj = collections.Counter(), collections.Counter()
row = re.compile(r"^\s+[0-9a-f]+\s+[0-9a-f]+\s+([0-9a-f]+)\s+\d+\s+(\S.*?):\((\.[^)]*)\)")
for line in open(path, errors="replace"):
    m = row.match(line)
    if not m: continue
    size, where = int(m.group(1), 16), m.group(2)
    a = re.match(r"(.*?\.a)\((.*?)\)$", where)
    lib[(a.group(1) if a else where).split("/")[-1]] += size
    obj[where.split("/")[-1]] += size
tot = sum(lib.values())
print("total mapped: %.2f MB" % (tot / 1048576))
print("-- by library --")
for k, v in lib.most_common(top): print("%9.1f KB  %5.1f%%  %s" % (v / 1024, 100.0 * v / max(tot, 1), k))
print("-- by object --")
for k, v in obj.most_common(top): print("%9.1f KB  %s" % (v / 1024, k))
