#! /usr/bin/env python3

# TODO add a check which ensures that the docs for a given version exist before
# building, this will be useful when running individual version builds from a CI
# pipeline which I may want to do to make outputs easier to cache

import json
import sys
from urllib.parse import urljoin

version_file = sys.argv[1]
base_url = sys.argv[2]
template_file = sys.argv[3]
switcher_out = sys.argv[4]
index_out = sys.argv[5]


def make_url(v: str) -> str:
    return urljoin(base_url, v)


def make_li(v: str) -> str:
    return f"<li><a href={v}/index.html>{v}</a></li>"


with open(version_file, "r") as f:
    versions = [v.strip() for v in f]

with open(template_file, "r") as f:
    template = f.read()


with open(switcher_out, "w") as f:
    latest_version = versions[0]
    switcher = [{"version": v, "url": make_url(v)} for v in versions]
    json.dump(switcher, f)

with open(index_out, "w") as f:
    version_list = "\n".join(make_li(v) for v in ["latest"] + versions)
    out = template.format(language="English", versions=version_list)
    f.write(out)
