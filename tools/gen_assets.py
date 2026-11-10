#!/usr/bin/env python3
"""gen_assets.py <input> <output.h> <name> [--gzip] [--strip-pem]  -> C array header."""
import gzip, re, sys
args = [a for a in sys.argv[1:] if not a.startswith("--")]
flags = {a for a in sys.argv[1:] if a.startswith("--")}
if len(args) != 3: sys.exit(__doc__)
data = open(args[0], "rb").read()
if "--strip-pem" in flags:
    blocks = re.findall(r"-----BEGIN CERTIFICATE-----.*?-----END CERTIFICATE-----", data.decode("ascii", "ignore"), re.S)
    data = ("\n".join(blocks) + "\n").encode()
if "--gzip" in flags: data = gzip.compress(data, 9, mtime=0)
n = args[2]
with open(args[1], "w") as f:
    f.write(f"#ifndef ASSETS_{n.upper()}_H\n#define ASSETS_{n.upper()}_H\n")
    f.write(f"static const unsigned char assets_{n}[] = {{" + ",".join(str(b) for b in data) + "};\n")
    f.write(f"static const unsigned int assets_{n}_len = {len(data)};\n#endif\n")
