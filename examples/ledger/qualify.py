import argparse
import http.client
import json
import os
import queue
import shutil
import sqlite3
import subprocess
import threading
import time
from contextlib import closing
from pathlib import Path
from urllib.parse import urlsplit


class Service:
    def __init__(self, binary, database, output, name):
        self.stdout = (output / f"{name}-stdout.txt").open("w")
        self.stderr = (output / f"{name}-stderr.txt").open("w")
        self.process = subprocess.Popen(
            [
                str(binary),
                "--address",
                "127.0.0.1:0",
                "--database",
                str(database),
                "--workers",
                "2",
                "--h2c-port",
                "0",
            ],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=self.stderr,
            text=True,
        )
        self.lines = queue.Queue()
        self.reader = threading.Thread(target=self.read_output, daemon=True)
        self.reader.start()
        self.url = None
        self.h2_url = None
        deadline = time.monotonic() + 10

        try:
            while self.url is None or self.h2_url is None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise RuntimeError("service startup exceeded deadline")

                line = self.lines.get(timeout=remaining)
                if line is None:
                    raise RuntimeError("service exited during startup")

                if line.startswith("HTTP/1: "):
                    self.url = line.removeprefix("HTTP/1: ").strip()
                if line.startswith("HTTP/2: "):
                    self.h2_url = line.removeprefix("HTTP/2: ").strip()
        except BaseException:
            self.close(force=True)
            raise

    def read_output(self):
        for line in self.process.stdout:
            self.stdout.write(line)
            self.stdout.flush()
            self.lines.put(line)

        self.lines.put(None)

    def close(self, force=False):
        if self.process.poll() is None:
            if force:
                self.process.kill()
            else:
                self.process.terminate()

        try:
            self.process.wait(timeout=15)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait()
            raise RuntimeError("service shutdown exceeded deadline") from None
        finally:
            self.reader.join(timeout=2)
            self.process.stdout.close()
            self.stdout.close()
            self.stderr.close()

        if not force and self.process.returncode != 0:
            raise RuntimeError(f"service exited with status {self.process.returncode}")


def request(url, path, payment=None):
    target = urlsplit(url)
    connection = http.client.HTTPConnection(target.hostname, target.port, timeout=5)
    body = None if payment is None else json.dumps(payment)
    method = "GET" if payment is None else "POST"

    try:
        connection.request(
            method,
            path,
            body,
            {"Content-Type": "application/json"},
        )
        response = connection.getresponse()
        data = response.read(4097)
        if len(data) > 4096:
            raise RuntimeError("response exceeded expected bound")

        return response.status, json.loads(data)
    finally:
        connection.close()


def accounting(database):
    with closing(sqlite3.connect(database)) as connection:
        balance = connection.execute("SELECT SUM(balance) FROM accounts").fetchone()[0]
        recorded = connection.execute(
            "SELECT COALESCE(SUM(CASE kind WHEN 'credit' THEN amount ELSE -amount END), 0) "
            "FROM transactions"
        ).fetchone()[0]
        count = connection.execute("SELECT COUNT(*) FROM transactions").fetchone()[0]

    if balance != recorded:
        raise RuntimeError(
            f"accounting mismatch: balances {balance}, transactions {recorded}"
        )

    return count


def run_load(urls, rate, duration, output, label, services):
    script = Path(__file__).with_name("load.py")
    process = subprocess.Popen(
        [
            os.sys.executable,
            str(script),
            *urls,
            "--rate",
            str(rate),
            "--duration",
            str(duration),
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    peak_rss = {}
    deadline = time.monotonic() + duration + 15

    try:
        while process.poll() is None:
            if time.monotonic() >= deadline:
                raise RuntimeError("load generator exceeded deadline")

            for service in services:
                pid = service.process.pid
                status = Path(f"/proc/{pid}/status")
                if not status.is_file():
                    continue

                for line in status.read_text().splitlines():
                    if line.startswith("VmRSS:"):
                        current = int(line.split()[1])
                        peak_rss[str(pid)] = max(peak_rss.get(str(pid), 0), current)
                        break

            time.sleep(0.05)

        stdout, stderr = process.communicate()
        if process.returncode != 0:
            raise RuntimeError(f"load generator failed: {stderr}")
    finally:
        if process.poll() is None:
            process.kill()
            process.communicate()

    metrics = json.loads(stdout)
    metrics["peak_service_rss_kib"] = peak_rss or None
    (output / f"{label}.json").write_text(json.dumps(metrics, indent=2) + "\n")

    if metrics["completed"] + metrics["dropped_before_send"] != metrics["scheduled"]:
        raise RuntimeError("load generator request counts do not conserve")

    return metrics


def qualify(args):
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    database = output / "ledger.sqlite3"
    services = []
    results = {}
    payment = {
        "amount": 1,
        "kind": "credit",
        "description": "qualify",
    }

    try:
        for index in range(2):
            service = Service(
                args.binary.resolve(), database, output, f"service-{index}"
            )
            services.append(service)

        urls = [service.url for service in services]
        results["load"] = []

        if args.http2:
            curl = shutil.which("curl")
            if curl is None:
                raise RuntimeError(
                    "HTTP/2 smoke checks require curl with HTTP/2 support"
                )

            for service in services:
                response = subprocess.run(
                    [
                        curl,
                        "--fail",
                        "--silent",
                        "--show-error",
                        "--http2-prior-knowledge",
                        "--max-time",
                        "5",
                        service.h2_url + "/accounts/1/statement",
                    ],
                    capture_output=True,
                    text=True,
                    check=True,
                )
                if json.loads(response.stdout)["balance"] != 0:
                    raise RuntimeError("initial HTTP/2 statement has a nonzero balance")

            results["http2_smoke"] = "PASS"

        for rate in args.rates:
            before = accounting(database)
            metrics = run_load(
                urls, rate, args.duration, output, f"rate-{rate}", services
            )
            persisted = accounting(database) - before
            successful = metrics["statuses"].get("200", 0)
            metrics["persisted_transactions"] = persisted
            metrics["persisted_minus_acknowledged"] = persisted - successful
            metrics["persisted_without_success_response"] = max(
                0, persisted - successful
            )
            metrics["client_timeouts"] = metrics["statuses"].get("client_timeout", 0)
            results["load"].append(metrics)
            (output / f"rate-{rate}.json").write_text(
                json.dumps(metrics, indent=2) + "\n"
            )

            transport_errors = metrics["statuses"].get("transport_error", 0)
            invalid_responses = metrics["statuses"].get("invalid_response", 0)

            if persisted != successful:
                raise RuntimeError(
                    "load accounting mismatch: "
                    f"{persisted} transactions persisted, {successful} HTTP 200 responses, "
                    f"{metrics['persisted_without_success_response']} persisted without a "
                    f"success response, {metrics['client_timeouts']} client timeouts, "
                    f"statuses {metrics['statuses']}"
                )

            if metrics["client_timeouts"] or transport_errors or invalid_responses:
                raise RuntimeError(
                    f"load had {metrics['client_timeouts']} client timeouts, "
                    f"{transport_errors} transport errors, and {invalid_responses} "
                    f"invalid responses at target {rate} requests per second"
                )

        with closing(sqlite3.connect(database, isolation_level=None)) as lock:
            lock.execute("BEGIN IMMEDIATE")
            before = accounting(database)

            try:
                status, _ = request(urls[0], "/accounts/1/transactions", payment)
                if status != 503:
                    raise RuntimeError(f"expected storage-busy 503, received {status}")
            finally:
                lock.execute("ROLLBACK")

        if accounting(database) != before:
            raise RuntimeError("rejected transaction changed persisted state")

        status, _ = request(urls[0], "/accounts/1/transactions", payment)
        if status != 200:
            raise RuntimeError("service did not recover after database contention")

        results["database_contention_recovery"] = "PASS"
        before = accounting(database)
        services[0].close(force=True)
        services[0] = None

        status, _ = request(urls[1], "/accounts/1/transactions", payment)
        if status != 200:
            raise RuntimeError("surviving instance stopped serving")

        restarted = Service(
            args.binary.resolve(), database, output, "service-restarted"
        )
        services[0] = restarted
        urls[0] = restarted.url

        if accounting(database) != before + 1:
            raise RuntimeError("acknowledged transactions did not survive restart")

        status, statement = request(urls[0], "/accounts/1/statement")
        if status != 200 or statement["balance"] < -statement["limit"]:
            raise RuntimeError("restarted service returned an invalid statement")

        results["crash_restart_recovery"] = "PASS"
        before = accounting(database)
        recovery = run_load(
            urls,
            args.recovery_rate,
            args.duration,
            output,
            "recovery",
            services,
        )
        persisted = accounting(database) - before
        successful = recovery["statuses"].get("200", 0)
        recovery["persisted_transactions"] = persisted
        recovery["persisted_minus_acknowledged"] = persisted - successful
        recovery["persisted_without_success_response"] = max(0, persisted - successful)
        recovery["client_timeouts"] = recovery["statuses"].get("client_timeout", 0)
        results["recovery"] = recovery
        (output / "recovery.json").write_text(json.dumps(recovery, indent=2) + "\n")

        if persisted != successful:
            raise RuntimeError(
                f"recovery accounting mismatch: {persisted} transactions persisted, "
                f"{successful} HTTP 200 responses, statuses {recovery['statuses']}"
            )

        if (
            recovery["client_timeouts"]
            or recovery["statuses"].get("transport_error", 0)
            or recovery["statuses"].get("invalid_response", 0)
        ):
            raise RuntimeError(
                f"recovery load had transport or response failures: {recovery['statuses']}"
            )

        if recovery["statuses"].get("200", 0) == 0:
            raise RuntimeError("no requests completed successfully after recovery")

        results["qualification"] = "PASS"
    except Exception as error:
        results["qualification"] = "FAIL"
        results["error"] = str(error)
        (output / "results.json").write_text(json.dumps(results, indent=2) + "\n")
        raise
    finally:
        cleanup_errors = []

        for service in services:
            if service is not None:
                try:
                    service.close()
                except (OSError, RuntimeError, subprocess.SubprocessError) as error:
                    cleanup_errors.append(str(error))

        if cleanup_errors:
            results["qualification"] = "FAIL"
            results["shutdown_errors"] = cleanup_errors
            (output / "results.json").write_text(json.dumps(results, indent=2) + "\n")
            raise RuntimeError("service cleanup failed: " + "; ".join(cleanup_errors))

    (output / "results.json").write_text(json.dumps(results, indent=2) + "\n")
    print(json.dumps(results, indent=2))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--binary", type=Path, default=Path("target/release/veylor-ledger")
    )
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--duration", type=int, default=5)
    parser.add_argument("--rates", type=int, nargs="+", default=[100, 500, 2000])
    parser.add_argument("--recovery-rate", type=int, default=100)
    parser.add_argument("--http2", action="store_true")
    args = parser.parse_args()

    if not 1 <= args.duration <= 3600:
        parser.error("duration must be 1..3600")
    if not 1 <= args.recovery_rate <= 100000:
        parser.error("recovery-rate must be 1..100000")
    if args.recovery_rate * args.duration > 1000000:
        parser.error("recovery rate may schedule at most 1000000 requests")
    if any(
        rate < 1 or rate > 100000 or rate * args.duration > 1000000
        for rate in args.rates
    ):
        parser.error(
            "rates must be 1..100000 with at most 1000000 scheduled requests per rate"
        )
    if len(set(args.rates)) != len(args.rates):
        parser.error("rates must be distinct")

    qualify(args)


if __name__ == "__main__":
    main()
