"""Puts every file under a folder into an Azurite container, each blob named by its
path on disk, signed with Shared Key by the standard library alone. The Azure lake
suite reads back a Delta table that a writer other than basalt put there.

Usage: azure_put.py <endpoint> <account> <key> <container> <prefix> <folder>
"""

import base64
import datetime
import hashlib
import hmac
import os
import sys
import urllib.parse
import urllib.request

API_VERSION = "2021-08-06"


def put(endpoint, account, key, container, blob, data):
    path = "/{}/{}/{}".format(account, container, urllib.parse.quote(blob))
    date = datetime.datetime.now(datetime.timezone.utc).strftime("%a, %d %b %Y %H:%M:%S GMT")
    headers = {"x-ms-blob-type": "BlockBlob", "x-ms-date": date, "x-ms-version": API_VERSION}
    canon_headers = "".join("{}:{}\n".format(k, headers[k]) for k in sorted(headers))
    length = str(len(data)) if data else ""
    ctype = "application/octet-stream"
    to_sign = "PUT\n\n\n{}\n\n{}\n\n\n\n\n\n\n{}/{}{}".format(length, ctype, canon_headers, account, path)
    sig = base64.b64encode(hmac.new(base64.b64decode(key), to_sign.encode(), hashlib.sha256).digest()).decode()
    headers["Content-Type"] = ctype
    headers["Authorization"] = "SharedKey {}:{}".format(account, sig)
    req = urllib.request.Request(endpoint + path, data=data, method="PUT", headers=headers)
    with urllib.request.urlopen(req) as resp:
        if resp.status != 201:
            sys.exit("put {}: HTTP {}".format(blob, resp.status))


def main():
    endpoint, account, key, container, prefix, folder = sys.argv[1:7]
    for root, _, files in os.walk(folder):
        for name in files:
            full = os.path.join(root, name)
            rel = os.path.relpath(full, folder).replace(os.sep, "/")
            with open(full, "rb") as f:
                put(endpoint, account, key, container, prefix + "/" + rel, f.read())


if __name__ == "__main__":
    main()
