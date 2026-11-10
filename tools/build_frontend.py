#!/usr/bin/env python3
"""build_frontend.py <frontend/index.html> <out.html>

Inlines every frontend/locales/<code>.json into the page (window.__I18N) so the payload still serves ONE file.
Locale files are the Crowdin source (en.json) and its translations (see .github/workflows/crowdin.yml)."""
import glob, json, os, sys
src, out = sys.argv[1], sys.argv[2]
html = open(src, encoding="utf-8").read()
loc = {}
for f in sorted(glob.glob(os.path.join(os.path.dirname(src), "locales", "*.json"))):
    code = os.path.basename(f)[:-5]
    data = json.load(open(f, encoding="utf-8"))
    data = {k: v for k, v in data.items() if isinstance(v, str) and v != ""}   # untranslated strings fall back to English in the UI
    loc[code] = data
if "en" not in loc: sys.exit("frontend/locales/en.json is missing")
blob = json.dumps(loc, ensure_ascii=False, separators=(",", ":")).replace("</", "<\\/")
if "<!--I18N-->" not in html: sys.exit("marker <!--I18N--> not found in " + src)
os.makedirs(os.path.dirname(out) or ".", exist_ok=True)
open(out, "w", encoding="utf-8").write(html.replace("<!--I18N-->", "<script>window.__I18N=" + blob + ";</script>", 1))
print("built %s: %d locale(s): %s" % (out, len(loc), ", ".join(sorted(loc))))
