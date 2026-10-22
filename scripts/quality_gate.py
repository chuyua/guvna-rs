#!/usr/bin/env python3
"""Real-binary, local-only adversarial load and RSS gate. Python stdlib only.

Linux x86_64 --release is authoritative. Windows can run behavior checks;
Windows memory results are explicitly not accepted as Linux memory evidence.
"""
from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import tempfile
import threading
import time
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


class Stub(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True
    request_queue_size = 256

    def __init__(self, role: str):
        self.role = role
        self.lock = threading.Lock()
        self.calls = 0
        super().__init__(("127.0.0.1", 0), StubHandler)

    def count(self):
        with self.lock:
            self.calls += 1


class StubHandler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_POST(self):
        self.server.count()
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length)
        try:
            query = json.loads(body)
            assert isinstance(query, dict)
        except (ValueError, AssertionError):
            self.send_response(400)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        if self.server.role == "primary":
            status, result = 503, {"error": "injected_primary_failure"}
        else:
            status, result = 200, {
                "id": "mock", "object": "chat.completion",
                "choices": [{"index": 0, "message": {
                    "role": "assistant", "content": "ok"}, "finish_reason": "stop"}],
                "seed": query.get("seed"),
            }
        payload = json.dumps(result, separators=(",", ":")).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        try:
            self.wfile.write(payload)
        except (ConnectionResetError, BrokenPipeError):
            pass


def port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


def proc_info(pid: int) -> dict:
    if sys.platform != "linux":
        return {}
    status = Path(f"/proc/{pid}/status").read_text()
    d = {}
    for line in status.splitlines():
        if line.startswith(("VmRSS:", "VmHWM:", "Threads:")):
            key, value = line.split(":", 1)
            d[key] = int(value.strip().split()[0])
    d["fd"] = len(list(Path(f"/proc/{pid}/fd").iterdir()))
    raw = Path(f"/proc/{pid}/stat").read_text()
    fields = raw[raw.rfind(")") + 2:].split()
    d["cpu_ticks"] = int(fields[11]) + int(fields[12])
    if "VmRSS" not in d or "Threads" not in d:
        raise RuntimeError("missing Linux RSS/thread sensors")
    return d


def check(condition: bool, message: str):
    if not condition:
        raise AssertionError(message)


def get_status(url: str, timeout=3.0) -> int:
    try:
        with urlopen(url, timeout=timeout) as resp:
            resp.read()
            return int(resp.status)
    except (OSError, URLError):
        return 0


def query(url: str, model: str, number: int) -> tuple[int, str]:
    data = json.dumps({"model": model, "messages": [
        {"role": "user", "content": "ping"}], "seed": number}).encode()
    req = Request(url, data=data, method="POST", headers={
        "Authorization": "Bearer only-for-local-test",
        "Content-Type": "application/json",
    })
    try:
        with urlopen(req, timeout=15) as resp:
            parsed = json.load(resp)
            if parsed.get("seed") != number or parsed["choices"][0]["message"]["content"] != "ok":
                return resp.status, "body_mismatch"
            return resp.status, "ok"
    except HTTPError as err:
        return err.code, "http_error"
    except (OSError, ValueError, KeyError, URLError) as err:
        return 0, type(err).__name__


def timed_query(url: str, model: str, number: int):
    start = time.monotonic()
    status, reason = query(url, model, number)
    return status, reason, (time.monotonic() - start) * 1000


def run_wave(url: str, model: str, requests: int, clients: int, pid: int):
    results: dict[str, int] = {}
    latencies = []
    max_rss = 0
    peak_rss = 0
    started = time.monotonic()
    with ThreadPoolExecutor(max_workers=clients) as ex:
        futures = [ex.submit(timed_query, url, model, i + (100000 if model == "chaos" else 0))
                   for i in range(requests)]
        for f in as_completed(futures):
            status, reason, latency = f.result()
            latencies.append(latency)
            key = f"{status}:{reason}"
            results[key] = results.get(key, 0) + 1
            p = proc_info(pid)
            max_rss = max(max_rss, p.get("VmRSS", 0))
            peak_rss = max(peak_rss, p.get("VmHWM", 0))
    elapsed = time.monotonic() - started
    check(results == {"200:ok": requests},
          f"{model}: expected {requests} valid 200, got {results}")
    latencies.sort()
    p95 = latencies[max(0, int((len(latencies) - 1) * .95))]
    p99 = latencies[max(0, int((len(latencies) - 1) * .99))]
    check(p95 <= 2000 and p99 <= 5000,
          f"{model}: high latency p95={p95:.1f}ms p99={p99:.1f}ms")
    return {"model": model, "requests": requests, "clients": clients, "seconds": round(elapsed, 3),
            "rps": round(requests / elapsed, 2), "p95_ms": round(p95, 2),
            "p99_ms": round(p99, 2), "max_rss_kib": max_rss,
            "peak_rss_kib": peak_rss, "statuses": results}


def idle(pid: int, seconds: float):
    samples = []
    cpu_first = proc_info(pid)
    begin = time.monotonic()
    while time.monotonic() - begin < seconds:
        time.sleep(0.5)
        samples.append(proc_info(pid))
    cpu_last = proc_info(pid)
    if sys.platform != "linux":
        return {"seconds": seconds, "memory_status": "unmeasured_windows"}
    ticks = os.sysconf("SC_CLK_TCK")
    percent = 100 * (cpu_last["cpu_ticks"] - cpu_first["cpu_ticks"]) / (
        ticks * (time.monotonic() - begin))
    return {
        "seconds": seconds,
        "rss_kib_max": max(x["VmRSS"] for x in samples),
        "rss_kib_last": samples[-1]["VmRSS"],
        "cpu_percent_single_core": round(percent, 3),
        "fds": samples[-1]["fd"], "threads": samples[-1]["Threads"],
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--binary", required=True)
    ap.add_argument("--requests", type=int, default=400)
    ap.add_argument("--clients", type=int, default=16)
    ap.add_argument("--idle-seconds", type=float, default=10)
    ap.add_argument("--max-idle-rss-mib", type=float, default=32)
    ap.add_argument("--max-peak-rss-mib", type=float, default=96)
    ap.add_argument("--max-rss-growth-mib", type=float, default=6)
    ap.add_argument("--out", default="quality-results.json")
    a = ap.parse_args()
    check(a.requests >= 1 and a.clients >= 1 and a.idle_seconds >= 3, "invalid test parameters")
    check(Path(a.binary).is_file(), f"binary not found: {a.binary}")
    check(sys.platform == "linux", "RSS/CPU gate requires Linux /proc; never pretend Windows is equivalent")
    summary = {"platform": sys.platform, "binary": str(Path(a.binary).name),
               "settings": {"clients": a.clients, "requests_per_wave": a.requests,
                            "idle_seconds": a.idle_seconds, "max_idle_rss_mib": a.max_idle_rss_mib,
                            "max_peak_rss_mib": a.max_peak_rss_mib},
               "passed": False}
    mock1, mock2 = Stub("primary"), Stub("healthy")
    thread1 = threading.Thread(target=mock1.serve_forever, daemon=True)
    thread2 = threading.Thread(target=mock2.serve_forever, daemon=True)
    thread1.start()
    thread2.start()
    proc = None
    try:
        with tempfile.TemporaryDirectory(prefix="guvna-quality-") as directory:
            gateway_port = port()
            cfg = Path(directory) / "config.yaml"
            cfg.write_text(f"""port: {gateway_port}
providers:
  - name: down
    type: openai
    base_url: http://127.0.0.1:{mock1.server_address[1]}
    key_env: DOWN_KEY
  - name: live
    type: openai
    base_url: http://127.0.0.1:{mock2.server_address[1]}
    key_env: LIVE_KEY
chains:
  - name: healthy
    steps:
      - provider: live
        model: fast
  - name: chaos
    steps:
      - provider: down
        model: down
      - provider: live
        model: fast
""", encoding="utf-8")
            env = dict(os.environ)
            env.update({"API_KEYS": "only-for-local-test", "DOWN_KEY": "fake-down-key",
                        "LIVE_KEY": "fake-live-key", "GUVNA_BIND_ADDR": "127.0.0.1"})
            started = time.monotonic()
            proc = subprocess.Popen(
                [str(Path(a.binary).resolve()), "serve", "-config", str(cfg), "-data", directory],
                stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, env=env)
            try:
                health = f"http://127.0.0.1:{gateway_port}/healthz"
                while time.monotonic() - started < 2:
                    if proc.poll() is not None:
                        raise AssertionError("gateway exited before healthcheck")
                    if get_status(health, timeout=0.15) == 200:
                        break
                    time.sleep(0.02)
                else:
                    raise AssertionError("gateway did not start within 2 seconds")
                summary["startup_seconds"] = round(time.monotonic() - started, 3)
                endpoint = f"http://127.0.0.1:{gateway_port}/v1/chat/completions"
                summary["idle_before"] = idle(proc.pid, a.idle_seconds)
                check(summary["idle_before"]["rss_kib_max"] <= a.max_idle_rss_mib * 1024,
                      f"startup idle RSS exceeds {a.max_idle_rss_mib} MiB")
                check(summary["idle_before"]["cpu_percent_single_core"] <= 1.0,
                      "idle CPU exceeds 1% single core")
                summary["wave1"] = run_wave(endpoint, "healthy", a.requests, a.clients, proc.pid)
                summary["post_wave1"] = idle(proc.pid, a.idle_seconds)
                summary["wave2"] = run_wave(endpoint, "chaos", a.requests, a.clients, proc.pid)
                summary["post_wave2"] = idle(proc.pid, a.idle_seconds)
                for wave in ("wave1", "wave2"):
                    check(summary[wave]["peak_rss_kib"] <= a.max_peak_rss_mib * 1024,
                          f"{wave} peak RSS exceeds {a.max_peak_rss_mib} MiB")
                for period in ("post_wave1", "post_wave2"):
                    check(summary[period]["rss_kib_max"] <= a.max_idle_rss_mib * 1024,
                          f"{period} RSS exceeds {a.max_idle_rss_mib} MiB")
                growth = summary["post_wave2"]["rss_kib_last"] - summary["post_wave1"]["rss_kib_last"]
                check(growth <= a.max_rss_growth_mib * 1024,
                      f"post-wave idle RSS grew {growth / 1024:.1f} MiB")
                check(summary["post_wave2"]["fds"] - summary["post_wave1"]["fds"] <= 8,
                      "FD leak between waves")
                check(summary["post_wave2"]["threads"] - summary["post_wave1"]["threads"] <= 8,
                      "thread leak between waves")
                check(mock1.calls == a.requests, f"chaos attempt count {mock1.calls} != {a.requests}")
                check(mock2.calls == a.requests * 2,
                      f"healthy provider call count {mock2.calls} != {a.requests * 2}")
                summary["upstream_calls"] = {"primary": mock1.calls, "healthy": mock2.calls}
                summary["passed"] = True
            finally:
                proc.terminate()
                try:
                    proc.communicate(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    proc.communicate(timeout=5)
    except Exception as exc:
        summary["error"] = f"{type(exc).__name__}: {exc}"
    finally:
        mock1.shutdown()
        mock2.shutdown()
        Path(a.out).write_text(json.dumps(summary, indent=2), encoding="utf-8")
        print(json.dumps(summary, indent=2))
    return 0 if summary["passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
