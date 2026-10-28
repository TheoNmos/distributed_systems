#!/usr/bin/env python3
"""Cliente S3 mínimo do laboratório. Uso: python /s3.py <nó 1|2|3> <comando> ..."""

import os
import statistics
import sys
import time

import boto3
from botocore.client import Config
from botocore.exceptions import BotoCoreError, ClientError

ENDPOINTS = {
    "1": "http://172.31.0.21:3900",
    "2": "http://172.31.0.22:3900",
    "3": "http://172.31.0.23:3900",
}
BUCKET = os.environ.get("S3_BUCKET", "sdlab")


def client(node):
    kwargs = {
        "signature_version": "s3v4",
        "s3": {"addressing_style": "path"},
        "connect_timeout": 5,
        "read_timeout": 90,
        "retries": {"max_attempts": 1},
    }
    try:
        cfg = Config(
            **kwargs,
            request_checksum_calculation="when_required",
            response_checksum_validation="when_required",
        )
    except TypeError:
        cfg = Config(**kwargs)
    return boto3.client(
        "s3",
        endpoint_url=ENDPOINTS[node],
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        region_name=os.environ.get("AWS_DEFAULT_REGION", "garage"),
        config=cfg,
    )


def fail(exc):
    if isinstance(exc, ClientError):
        err = exc.response.get("Error", {})
        code = err.get("Code", "ClientError")
        msg = err.get("Message", str(exc))
        status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode", "")
        print(f"ERR status={status} code={code} message={msg}")
    else:
        print(f"ERR type={type(exc).__name__} message={exc}")
    return 1


def cmd_put(node, key, value):
    started = time.perf_counter()
    client(node).put_object(Bucket=BUCKET, Key=key, Body=value.encode())
    elapsed = (time.perf_counter() - started) * 1000
    print(f"PUT ok node={node} key={key} bytes={len(value.encode())} elapsed_ms={elapsed:.2f}")


def cmd_putmb(node, key, mib):
    body = b"G" * (int(mib) * 1024 * 1024)
    started = time.perf_counter()
    client(node).put_object(Bucket=BUCKET, Key=key, Body=body)
    elapsed = (time.perf_counter() - started) * 1000
    print(f"PUT ok node={node} key={key} bytes={len(body)} elapsed_ms={elapsed:.2f}")


def cmd_get(node, key):
    started = time.perf_counter()
    obj = client(node).get_object(Bucket=BUCKET, Key=key)
    value = obj["Body"].read()
    elapsed = (time.perf_counter() - started) * 1000
    text = value.decode(errors="replace")
    print(f"GET ok node={node} key={key} bytes={len(value)} elapsed_ms={elapsed:.2f} value={text}")


def list_keys(s3):
    keys = []
    token = None
    while True:
        params = {"Bucket": BUCKET}
        if token:
            params["ContinuationToken"] = token
        page = s3.list_objects_v2(**params)
        keys.extend(item["Key"] for item in page.get("Contents", []))
        if not page.get("IsTruncated"):
            return keys
        token = page.get("NextContinuationToken")


def cmd_ls(node):
    started = time.perf_counter()
    keys = list_keys(client(node))
    elapsed = (time.perf_counter() - started) * 1000
    print(f"LS ok node={node} count={len(keys)} elapsed_ms={elapsed:.2f}")
    for key in keys:
        print(f"LS key={key}")


def cmd_bench(node, count):
    s3 = client(node)
    puts = []
    gets = []
    for i in range(int(count)):
        key = f"bench/{node}/{time.time_ns()}-{i}"
        body = b"x" * 1024
        started = time.perf_counter()
        s3.put_object(Bucket=BUCKET, Key=key, Body=body)
        puts.append((time.perf_counter() - started) * 1000)
        started = time.perf_counter()
        s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
        gets.append((time.perf_counter() - started) * 1000)
    put_mean = statistics.fmean(puts)
    get_mean = statistics.fmean(gets)
    put_sd = statistics.pstdev(puts) if len(puts) > 1 else 0.0
    get_sd = statistics.pstdev(gets) if len(gets) > 1 else 0.0
    print(
        f"BENCH n={count} node={node} "
        f"put_mean_ms={put_mean:.2f} put_stdev_ms={put_sd:.2f} "
        f"get_mean_ms={get_mean:.2f} get_stdev_ms={get_sd:.2f}"
    )
    print("BENCH_PUT_MS " + " ".join(f"{v:.2f}" for v in puts))
    print("BENCH_GET_MS " + " ".join(f"{v:.2f}" for v in gets))


def cmd_fanout(key, rounds):
    nodes = {name: client(name) for name in ("1", "2", "3")}
    stale = 0
    for i in range(int(rounds)):
        value = f"v{i}-{time.time_ns()}"
        started = time.perf_counter()
        nodes["1"].put_object(Bucket=BUCKET, Key=key, Body=value.encode())
        put_ms = (time.perf_counter() - started) * 1000
        for name in ("2", "3"):
            started = time.perf_counter()
            got = nodes[name].get_object(Bucket=BUCKET, Key=key)["Body"].read().decode()
            get_ms = (time.perf_counter() - started) * 1000
            match = got == value
            stale += not match
            print(
                f"FANOUT i={i} reader={name} match={str(match).lower()} "
                f"put_ms={put_ms:.2f} get_ms={get_ms:.2f} expected={value} got={got}"
            )
    print(f"FANOUT_SUMMARY stale={stale} reads={int(rounds) * 2}")


def cmd_lscheck(rounds):
    writer = client("1")
    reader = client("2")
    missing = 0
    for i in range(int(rounds)):
        key = f"lscheck/{time.time_ns()}-{i}"
        writer.put_object(Bucket=BUCKET, Key=key, Body=b"v")
        keys = list_keys(reader)
        present = key in keys
        missing += not present
        print(f"LSCHECK i={i} key={key} present_on_2={str(present).lower()}")
    print(f"LSCHECK_SUMMARY missing={missing} n={rounds}")


def main(argv):
    if len(argv) < 3:
        print("uso: python /s3.py <1|2|3> put|get|ls|bench|putmb|fanout|lscheck ...", file=sys.stderr)
        return 2
    node, command = argv[1], argv[2]
    if node not in ENDPOINTS and command not in {"fanout", "lscheck"}:
        print(f"nó inválido: {node}", file=sys.stderr)
        return 2
    try:
        if command == "put":
            cmd_put(node, argv[3], " ".join(argv[4:]))
        elif command == "get":
            cmd_get(node, argv[3])
        elif command == "ls":
            cmd_ls(node)
        elif command == "bench":
            cmd_bench(node, argv[3] if len(argv) > 3 else "20")
        elif command == "putmb":
            cmd_putmb(node, argv[3], argv[4])
        elif command == "fanout":
            cmd_fanout(argv[3], argv[4] if len(argv) > 4 else "20")
        elif command == "lscheck":
            cmd_lscheck(argv[3] if len(argv) > 3 else "10")
        else:
            print(f"comando desconhecido: {command}", file=sys.stderr)
            return 2
    except (ClientError, BotoCoreError, OSError) as exc:
        return fail(exc)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
