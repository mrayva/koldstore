#!/usr/bin/env python3
"""Minimal dependency-free S3 client (SigV4, path-style) for scripts/backup-restore-drill.sh.

Only what the drill needs, so it can run against any S3-compatible server (MinIO, ...) without
installing aws-cli / mc / boto3:  mb | ls | get | put | rm | size.

Environment: S3_ENDPOINT (default http://127.0.0.1:19090), S3_ACCESS_KEY, S3_SECRET_KEY,
S3_REGION (default us-east-1).

  s3_tool.py mb  BUCKET
  s3_tool.py ls  BUCKET [PREFIX]          # one "key<TAB>size" per line
  s3_tool.py get BUCKET KEY FILE
  s3_tool.py put BUCKET KEY FILE
  s3_tool.py rm  BUCKET KEY
  s3_tool.py size BUCKET KEY              # prints the byte size, or exits 3 if the key is absent
"""
import datetime
import hashlib
import hmac
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET

ENDPOINT = os.environ.get("S3_ENDPOINT", "http://127.0.0.1:19090")
ACCESS = os.environ.get("S3_ACCESS_KEY", "minioadmin")
SECRET = os.environ.get("S3_SECRET_KEY", "minioadmin")
REGION = os.environ.get("S3_REGION", "us-east-1")


def _sign(key, msg):
    return hmac.new(key, msg.encode(), hashlib.sha256).digest()


def request(method, path, query="", body=b""):
    """Signed request; returns (status, headers, body)."""
    url = urllib.parse.urlparse(ENDPOINT)
    host = url.netloc
    now = datetime.datetime.now(datetime.timezone.utc)
    amz_date = now.strftime("%Y%m%dT%H%M%SZ")
    date = now.strftime("%Y%m%d")
    payload_hash = hashlib.sha256(body).hexdigest()
    canonical_uri = urllib.parse.quote(path, safe="/-_.~")
    headers = {"host": host, "x-amz-content-sha256": payload_hash, "x-amz-date": amz_date}
    signed = ";".join(sorted(headers))
    canonical_headers = "".join(f"{k}:{headers[k]}\n" for k in sorted(headers))
    canonical = "\n".join([method, canonical_uri, query, canonical_headers, signed, payload_hash])
    scope = f"{date}/{REGION}/s3/aws4_request"
    to_sign = "\n".join(["AWS4-HMAC-SHA256", amz_date, scope, hashlib.sha256(canonical.encode()).hexdigest()])
    k = _sign(("AWS4" + SECRET).encode(), date)
    for part in (REGION, "s3", "aws4_request"):
        k = _sign(k, part)
    signature = hmac.new(k, to_sign.encode(), hashlib.sha256).hexdigest()
    auth = f"AWS4-HMAC-SHA256 Credential={ACCESS}/{scope}, SignedHeaders={signed}, Signature={signature}"
    req = urllib.request.Request(
        f"{ENDPOINT}{canonical_uri}" + (f"?{query}" if query else ""),
        data=body if method in ("PUT", "POST") else None,
        method=method,
        headers={**{k.title(): v for k, v in headers.items() if k != "host"}, "Authorization": auth},
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return resp.status, dict(resp.headers), resp.read()
    except urllib.error.HTTPError as err:
        return err.code, dict(err.headers), err.read()


def list_keys(bucket, prefix=""):
    out, token = [], None
    while True:
        params = {"list-type": "2", "prefix": prefix}
        if token:
            params["continuation-token"] = token
        query = "&".join(f"{urllib.parse.quote(k, safe='')}={urllib.parse.quote(v, safe='')}" for k, v in sorted(params.items()))
        status, _, body = request("GET", f"/{bucket}", query)
        if status != 200:
            sys.exit(f"list failed: {status} {body[:200]!r}")
        root = ET.fromstring(body)
        ns = {"s": root.tag.split("}")[0].strip("{")}
        for item in root.findall("s:Contents", ns):
            out.append((item.find("s:Key", ns).text, int(item.find("s:Size", ns).text)))
        if root.findtext("s:IsTruncated", default="false", namespaces=ns) == "true":
            token = root.findtext("s:NextContinuationToken", namespaces=ns)
        else:
            return out


def main(argv):
    if len(argv) < 3:
        sys.exit(__doc__)
    op, bucket = argv[1], argv[2]
    if op == "mb":
        status, _, body = request("PUT", f"/{bucket}")
        if status not in (200, 409):  # 409: already owned by you
            sys.exit(f"mb failed: {status} {body[:200]!r}")
    elif op == "ls":
        for key, size in list_keys(bucket, argv[3] if len(argv) > 3 else ""):
            print(f"{key}\t{size}")
    elif op == "get":
        status, _, body = request("GET", f"/{bucket}/{argv[3]}")
        if status != 200:
            sys.exit(f"get failed: {status}")
        open(argv[4], "wb").write(body)
    elif op == "put":
        status, _, body = request("PUT", f"/{bucket}/{argv[3]}", body=open(argv[4], "rb").read())
        if status != 200:
            sys.exit(f"put failed: {status} {body[:200]!r}")
    elif op == "rm":
        status, _, body = request("DELETE", f"/{bucket}/{argv[3]}")
        if status not in (200, 204):
            sys.exit(f"rm failed: {status} {body[:200]!r}")
    elif op == "size":
        status, headers, _ = request("HEAD", f"/{bucket}/{argv[3]}")
        if status == 404:
            sys.exit(3)
        if status != 200:
            sys.exit(f"head failed: {status}")
        print(headers.get("Content-Length"))
    else:
        sys.exit(__doc__)


if __name__ == "__main__":
    main(sys.argv)
