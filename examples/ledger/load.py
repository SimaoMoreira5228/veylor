import argparse
import concurrent.futures
import http.client
import json
import math
import threading
import time
from collections import Counter
from urllib.parse import urlsplit


def arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("urls", nargs="+")
    parser.add_argument("--rate", type=int, default=100)
    parser.add_argument("--duration", type=int, default=10)
    parser.add_argument("--concurrency", type=int, default=32)
    args = parser.parse_args()

    if not 1 <= args.rate <= 100000 or not 1 <= args.duration <= 3600:
        parser.error("rate must be 1..100000 and duration must be 1..3600")
    if args.rate * args.duration > 1000000:
        parser.error("a run may schedule at most 1000000 requests")
    if not 1 <= args.concurrency <= 1024:
        parser.error("concurrency must be 1..1024")

    targets = [urlsplit(url) for url in args.urls]
    for target in targets:
        if (
            target.scheme != "http"
            or not target.hostname
            or target.username
            or target.password
            or target.path not in ("", "/")
            or target.query
            or target.fragment
        ):
            parser.error(
                "targets must be HTTP origins, for example http://127.0.0.1:9999"
            )

    return args, targets


def run(args, targets):
    local = threading.local()
    payload = json.dumps(
        {
            "amount": 1,
            "kind": "credit",
            "description": "load",
        }
    ).encode()

    def send(index, scheduled):
        if not hasattr(local, "connections"):
            local.connections = {}

        key = index % len(targets)
        target = targets[key]
        connection = local.connections.get(key)
        if connection is None:
            connection = http.client.HTTPConnection(
                target.hostname, target.port or 80, timeout=2
            )
            local.connections[key] = connection

        try:
            account = index % 5 + 1
            connection.request(
                "POST",
                f"/accounts/{account}/transactions",
                payload,
                {"Content-Type": "application/json"},
            )
            response = connection.getresponse()
            body = response.read(4097)
            if len(body) > 4096:
                raise ValueError("response exceeds expected bound")

            if response.status == 200:
                decoded = json.loads(body)

                if not isinstance(decoded, dict):
                    raise ValueError("balance response must be an object")

                if (
                    type(decoded.get("balance")) is not int
                    or type(decoded.get("limit")) is not int
                    or decoded["balance"] < -decoded["limit"]
                ):
                    raise ValueError("invalid balance response")

            return str(response.status), time.perf_counter() - scheduled
        except TimeoutError:
            connection.close()
            local.connections.pop(key, None)

            return "client_timeout", time.perf_counter() - scheduled
        except (OSError, http.client.HTTPException):
            connection.close()
            local.connections.pop(key, None)

            return "transport_error", time.perf_counter() - scheduled
        except ValueError:
            connection.close()
            local.connections.pop(key, None)

            return "invalid_response", time.perf_counter() - scheduled

    statuses = Counter()
    latencies = []
    pending = set()
    dropped = 0
    scheduled_count = args.rate * args.duration
    started = time.perf_counter()

    def collect(completed):
        for future in completed:
            status, elapsed = future.result()
            statuses[status] += 1
            latencies.append(elapsed * 1000)

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=args.concurrency
    ) as executor:
        for index in range(scheduled_count):
            scheduled = started + index / args.rate
            wait = scheduled - time.perf_counter()
            if wait > 0:
                time.sleep(wait)

            completed = {future for future in pending if future.done()}
            pending.difference_update(completed)
            collect(completed)

            if (
                len(pending) >= args.concurrency
                or time.perf_counter() - scheduled >= 1 / args.rate
            ):
                dropped += 1
                continue

            pending.add(executor.submit(send, index, scheduled))

        collect(concurrent.futures.as_completed(pending))

    elapsed = time.perf_counter() - started
    latencies.sort()

    def percentile(fraction):
        if not latencies:
            return None

        index = max(0, math.ceil(len(latencies) * fraction) - 1)
        return round(latencies[index], 3)

    print(
        json.dumps(
            {
                "targets": args.urls,
                "target_requests_per_second": args.rate,
                "scheduled": scheduled_count,
                "completed": len(latencies),
                "dropped_before_send": dropped,
                "statuses": dict(statuses),
                "elapsed_seconds": round(elapsed, 3),
                "successful_requests_per_second": round(statuses["200"] / elapsed, 3),
                "scheduled_to_completion_ms": {
                    "p50": percentile(0.5),
                    "p95": percentile(0.95),
                    "p99": percentile(0.99),
                    "max": round(latencies[-1], 3) if latencies else None,
                },
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    run(*arguments())
