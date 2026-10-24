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
import sqlite3
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
        self.streaming_seen = 0
        self.faults = {"503": 0, "invalid_200": 0, "truncated_200": 0}
        self.structured = []
        super().__init__(("127.0.0.1", 0), StubHandler)

    def count(self):
        with self.lock:
            self.calls += 1

    def record_fault(self, name):
        with self.lock:
            self.faults[name] += 1


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
        requested_format = query.get("response_format")
        json_mode = isinstance(requested_format, dict) and (
            requested_format.get("type") in ("json_object", "json_schema"))
        if self.server.role == "rate_limited":
            payload = b'{"error":"upstream_throttled"}'
            self.send_response(429)
            self.send_header("Retry-After", "3600")
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)
            return
        if self.server.role == "stream_only":
            # This simulated provider has no JSON Chat response capability:
            # it rejects every request without stream:true. Invalid terminal
            # streams must be handled by the next candidate, not fabricated
            # into a successful JSON response.
            if query.get("stream") is not True:
                self.send_response(400)
                self.send_header("Content-Length", "0")
                self.end_headers()
                return
            with self.server.lock:
                self.server.streaming_seen += 1
            number = int(query.get("seed", 0))
            first = {"id": "stream-only-test", "object": "chat.completion.chunk",
                     "created": 10, "model": "synthetic",
                     "choices": [{"index":0, "delta":{"role":"assistant",
                     "content":"stream-ok"}, "finish_reason":None}]}
            finish = {"choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}
            if number % 7 == 0:
                if number % 2 == 0:
                    # Audio cannot be represented in JSON Chat text fields.
                    # Must fail over, not fabricate a successful completion.
                    first["choices"][0]["delta"]["audio"] = {"data": "fake-audio"}
                    payload = ("data: " + json.dumps(first) + "\n\ndata: " +
                               json.dumps(finish) + "\n\ndata: [DONE]\n\n").encode()
                else:
                    # Premature DONE before finish_reason: fail closed.
                    payload = ("data: " + json.dumps(first) +
                               "\n\ndata: [DONE]\n\n").encode()
            else:
                if number % 11 == 0:
                    first["choices"][0]["delta"]["content"] = None
                    first["choices"][0]["delta"]["refusal"] = "request declined"
                elif number % 13 == 0:
                    first["choices"][0]["delta"]["reasoning_content"] = "private reasoning"
                payload = ("data: " + json.dumps(first) + "\n\ndata: " +
                           json.dumps(finish) + "\n\ndata: [DONE]\n\n").encode()
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream; charset=utf-8")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            try:
                midpoint = len(payload) // 3
                self.wfile.write(payload[:midpoint])
                self.wfile.flush()
                self.wfile.write(payload[midpoint:])
            except (ConnectionResetError, BrokenPipeError):
                pass
            return
        if self.server.role == "healthy" and json_mode:
            with self.server.lock:
                self.server.structured.append(requested_format)
        messages = query.get("messages") or []
        first_text = (messages[0].get("content") if messages and
                      isinstance(messages[0], dict) else None)
        scripted_content = ("not JSON" if first_text == "badjson" else
                            '{"ok":true}' if json_mode else "ok")
        if self.server.role == "healthy" and query.get("stream") is True:
            first = {"id": "quality-stream", "created": 1, "choices": [{
                "index": 0, "delta": {"role": "assistant", "content": scripted_content},
                "finish_reason": None}]}
            finish = {"id": "quality-stream", "choices": [{
                "index": 0, "delta": {}, "finish_reason": "stop"}]}
            payload = (
                "data: " + json.dumps(first, separators=(",", ":")) + "\n\n"
                + "data: " + json.dumps(finish, separators=(",", ":"))
                + "\n\ndata: [DONE]\n\n"
            ).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            try:
                # Fragment deterministic subsets of live SSE traffic across
                # JSON fields and even the terminal marker. No fake success
                # from a buffered one-shot mock is accepted as the only proof.
                request_num = int(query.get("metadata", {}).get("request", "0"))
                if request_num % 13 == 0:
                    cuts = (1, len(payload) // 3, len(payload) - 1)
                    previous = 0
                    for cut in cuts:
                        self.wfile.write(payload[previous:cut])
                        self.wfile.flush()
                        time.sleep(0.001)
                        previous = cut
                    self.wfile.write(payload[previous:])
                else:
                    self.wfile.write(payload)
            except (ConnectionResetError, BrokenPipeError):
                pass
            return
        if self.server.role == "primary":
            seed = int(query.get("seed", 0))
            if seed % 17 == 0:
                self.server.record_fault("truncated_200")
                status, result = 200, {"choices": [
                    {"index": 0, "message": {"role": "assistant", "content": "partial"},
                     "finish_reason": "stop"}]}
            elif seed % 13 == 0:
                self.server.record_fault("invalid_200")
                status, result = 200, {"choices": []}
            else:
                self.server.record_fault("503")
                status, result = 503, {"error": "injected_primary_failure"}
        else:
            status, result = 200, {
                "id": "mock", "object": "chat.completion",
                "choices": [{"index": 0, "message": {
                    "role": "assistant", "content": scripted_content}, "finish_reason": "stop"}],
                "seed": query.get("seed"),
            }
        payload = json.dumps(result, separators=(",", ":")).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        short = self.server.role == "primary" and int(query.get("seed", 0)) % 17 == 0
        self.send_header("Content-Length", str(len(payload) + (37 if short else 0)))
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



def timed_responses_sse_query(url: str, number: int):
    start = time.monotonic()
    data = json.dumps({"model": "healthy", "input": "ping", "stream": True,
                       "metadata": {"request": str(number)}}).encode()
    request = Request(url, data=data, method="POST", headers={
        "Authorization": "Bearer only-for-local-test",
        "Content-Type": "application/json",
    })
    try:
        with urlopen(request, timeout=15) as resp:
            if resp.status != 200 or resp.headers.get_content_type() != "text/event-stream":
                return resp.status, "wrong_stream_response", (time.monotonic() - start) * 1000
            raw = resp.read(1 << 20).decode("utf-8")
            events = [json.loads(line[6:]) for line in raw.splitlines()
                      if line.startswith("data: ")]
            completed = [event for event in events
                         if event.get("type") == "response.completed"]
            if (len(completed) != 1 or len(events) < 5
                    or any(event.get("sequence_number") != index
                           for index, event in enumerate(events))
                    or any(event.get("type") in ("error", "response.failed")
                           for event in events)):
                return resp.status, "invalid_sse_events", (time.monotonic() - start) * 1000
            response = completed[0]["response"]
            if (response["output_text"] != "ok"
                    or response["status"] != "completed"
                    or response["metadata"]["request"] != str(number)):
                return resp.status, "wrong_stream_payload", (time.monotonic() - start) * 1000
            return resp.status, "ok", (time.monotonic() - start) * 1000
    except HTTPError as err:
        return err.code, "http_error", (time.monotonic() - start) * 1000
    except (OSError, ValueError, KeyError, URLError, UnicodeError) as err:
        return 0, type(err).__name__, (time.monotonic() - start) * 1000


def run_responses_sse_wave(url: str, requests: int, clients: int, pid: int):
    results = {}
    latencies = []
    peak_rss = 0
    with ThreadPoolExecutor(max_workers=clients) as pool:
        futures = [pool.submit(timed_responses_sse_query, url, i)
                   for i in range(requests)]
        for future in as_completed(futures):
            status, reason, elapsed_ms = future.result()
            key = f"{status}:{reason}"
            results[key] = results.get(key, 0) + 1
            latencies.append(elapsed_ms)
            peak_rss = max(peak_rss, proc_info(pid)["VmHWM"])
    check(results == {"200:ok": requests},
          f"Responses SSE wave: expected {requests} valid complete streams, got {results}")
    latencies.sort()
    p95 = latencies[max(0, int((len(latencies) - 1) * .95))]
    check(p95 <= 4000, f"Responses SSE latency P95 {p95:.1f}ms exceeded 4000ms")
    return {"requests": requests, "clients": clients,
            "peak_rss_kib": peak_rss, "p95_ms": round(p95, 2),
            "statuses": results}

def verify_metrics(url: str, requests: int, sse_requests: int) -> dict:
    # The observability endpoint must not be public even on a loopback deployment.
    try:
        with urlopen(url, timeout=3) as _:
            raise AssertionError("metrics endpoint leaked without bearer authorization")
    except HTTPError as error:
        check(error.code == 401, f"unauthenticated metrics returned {error.code}")

    req = Request(url, headers={"Authorization": "Bearer only-for-local-test"})
    with urlopen(req, timeout=3) as response:
        check(response.status == 200, "metrics returned non-200")
        check(response.headers.get_content_type() == "text/plain", "metrics MIME mismatch")
        raw = response.read(8 << 10).decode("utf-8")
    check("fake-live-key" not in raw and "fake-down-key" not in raw
          and "only-for-local-test" not in raw and '{' not in raw,
          "metrics contained credentials or labels")
    values = {}
    for line in raw.splitlines():
        if line.startswith("guvna_"):
            metric, _, value = line.partition(" ")
            check(metric not in values, f"duplicate metric: {metric}")
            values[metric] = int(value)

    expected = {
        "guvna_requests_dispatched_total": requests * 2 + sse_requests,
        "guvna_upstream_attempts_total": requests * 3 + sse_requests,
        "guvna_candidate_fallbacks_total": requests,
        "guvna_upstream_success_total": requests * 2 + sse_requests,
        "guvna_chat_stream_completed_total": sse_requests,
        "guvna_responses_stream_completed_total": sse_requests,
        "guvna_responses_stream_incomplete_total": 0,
        "guvna_responses_stream_failed_total": 0,
        "guvna_responses_stream_cancelled_total": 0,
        "guvna_chat_stream_failed_total": 0,
        "guvna_chat_stream_cancelled_total": 0,
        "guvna_admission_rejections_total": 0,
        "guvna_active_requests": 0,
        "guvna_rate_limit_exhaustions_total": 0,
        "guvna_circuit_bypasses_total": 0,
        "guvna_circuit_openings_total": 0,
        "guvna_circuit_probe_success_total": 0,
        "guvna_circuit_probe_failures_total": 0,
        "guvna_circuit_open_providers": 0,
    }
    for metric, wanted in expected.items():
        check(values.get(metric) == wanted,
              f"{metric}: expected {wanted}, got {values.get(metric)}")
    check(values.get("guvna_upstream_invalid_responses_total", 0) > 0,
          "invalid primary upstream responses were not observed")
    check(values.get("guvna_upstream_http_failures_total", 0) > 0,
          "provider HTTP failures were not observed")
    return values


def exercise_opt_in_circuit_gate(binary: str, config: Path, root: str, env: dict,
                                 mock1: Stub, mock2: Stub) -> dict:
    """Run a separate release process so the legacy/default-disabled gate
    remains byte-for-byte comparable to previous runs."""
    breaker_port = port()
    breaker_cfg = Path(root) / "circuit-enabled.yaml"
    original = config.read_text(encoding="utf-8")
    check("    key_env: DOWN_KEY\n" in original, "missing breaker insertion anchor")
    revised = original.replace(f"port: {original.splitlines()[0].split(': ')[1]}",
                               f"port: {breaker_port}", 1)
    revised = revised.replace("    key_env: DOWN_KEY\n",
                              "    key_env: DOWN_KEY\n"
                              "    circuit_breaker:\n"
                              "      enabled: true\n"
                              "      failure_threshold: 3\n"
                              "      cooldown: 30s\n"
                              "      max_cooldown: 30s\n", 1)
    breaker_cfg.write_text(revised, encoding="utf-8")
    breaker_data = Path(root) / "breaker-data"
    proc = subprocess.Popen(
        [str(Path(binary).resolve()), "serve", "-config", str(breaker_cfg),
         "-data", str(breaker_data)],
        stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, env=env)
    try:
        health = f"http://127.0.0.1:{breaker_port}/healthz"
        started = time.monotonic()
        while time.monotonic() - started < 2:
            check(proc.poll() is None, "breaker gateway terminated at startup")
            if get_status(health, timeout=0.1) == 200:
                break
            time.sleep(0.02)
        else:
            raise AssertionError("breaker gateway startup timeout")
        initial_primary = mock1.calls
        initial_healthy = mock2.calls
        target = f"http://127.0.0.1:{breaker_port}/v1/chat/completions"
        # Nonconcurrent calls make the expected 3-attempt threshold exact.
        wave = run_wave(target, "chaos", 64, 1, proc.pid)
        avoided = mock1.calls - initial_primary
        check(avoided == 3, f"breaker failed to suppress primary requests: {avoided}/64")
        check(mock2.calls - initial_healthy == 64,
              "breaker chain did not preserve healthy fallback")
        req = Request(f"http://127.0.0.1:{breaker_port}/metrics",
                      headers={"Authorization": "Bearer only-for-local-test"})
        with urlopen(req, timeout=3) as response:
            metrics = {}
            for line in response.read(8 << 10).decode("utf-8").splitlines():
                if line.startswith("guvna_"):
                    name, _, value = line.partition(" ")
                    metrics[name] = int(value)
        expected = {
            "guvna_requests_dispatched_total": 64,
            "guvna_upstream_attempts_total": 67,
            "guvna_upstream_success_total": 64,
            "guvna_candidate_fallbacks_total": 64,
            "guvna_circuit_openings_total": 1,
            "guvna_circuit_bypasses_total": 61,
            "guvna_circuit_probe_success_total": 0,
            "guvna_circuit_probe_failures_total": 0,
            "guvna_circuit_open_providers": 1,
            "guvna_active_requests": 0,
        }
        for name, wanted in expected.items():
            check(metrics.get(name) == wanted,
                  f"breaker metric mismatch {name}: {metrics.get(name)} != {wanted}")
        # Read /proc after the new wave; never silently skip RSS/leak checks.
        usage = proc_info(proc.pid)
        check(usage["VmRSS"] <= 16 * 1024, "breaker instance RSS exceeded 16 MiB")
        check(usage["fd"] <= 20, "breaker instance leaked file descriptors")
        check(usage["Threads"] <= 12, "breaker instance leaked threads")
        return {"wave": wave, "primary_attempts": avoided,
                "requests_avoiding_primary": 61,
                "metrics": metrics,
                "rss_kib": usage["VmRSS"], "fds": usage["fd"],
                "threads": usage["Threads"]}
    finally:
        proc.terminate()
        try:
            proc.communicate(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.communicate(timeout=5)

def verify_release_stream_only_aggregation(binary: str, root: str,
                                           env: dict, live_stub: Stub) -> dict:
    """64 real Linux release JSON requests through a strict SSE-only source.
    10 intentionally truncated/invalid streams must take normal failover,
    without returning partial content, leaking secrets or retaining buffers.
    """
    upstream = Stub("stream_only")
    thread = threading.Thread(target=upstream.serve_forever, daemon=True)
    thread.start()
    gateway_port = port()
    config = Path(root) / "sse-only-enabled.yaml"
    config.write_text(f"""port: {gateway_port}
providers:
  - name: streaming
    type: openai
    base_url: http://127.0.0.1:{upstream.server_address[1]}
    key_env: STREAM_ONLY_KEY
    stream_only: true
  - name: backup
    type: openai
    base_url: http://127.0.0.1:{live_stub.server_address[1]}
    key_env: LIVE_KEY
chains:
  - name: aggregate
    steps:
      - provider: streaming
        model: only-stream
      - provider: backup
        model: regular-json
""", encoding="utf-8")
    data = Path(root) / "aggregate-data"
    data.mkdir()
    gateway_env = dict(env, GUVNA_LOG_EVENTS="1", STREAM_ONLY_KEY="fake-stream-key")
    proc = subprocess.Popen(
        [str(Path(binary).resolve()), "serve", "-config", str(config),
         "-data", str(data)],
        stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, env=gateway_env)
    captured = b""
    try:
        health = f"http://127.0.0.1:{gateway_port}/healthz"
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline:
            check(proc.poll() is None, "SSE-only gateway stopped during startup")
            if get_status(health, timeout=0.15) == 200:
                break
            time.sleep(0.02)
        else:
            raise AssertionError("SSE-only release gateway did not start")

        baseline_live = live_stub.calls
        api = f"http://127.0.0.1:{gateway_port}/v1/chat/completions"

        def call_seed(seed: int) -> tuple[int, str]:
            payload = json.dumps({
                "model": "aggregate",
                "messages": [{"role": "user", "content": "ping"}],
                "seed": seed,
            }).encode()
            req = Request(api, data=payload, method="POST", headers={
                "Authorization": "Bearer only-for-local-test",
                "Content-Type": "application/json"
            })
            with urlopen(req, timeout=12) as resp:
                check(resp.headers.get_content_type() == "application/json",
                      "non-stream consumer received upstream SSE framing")
                answer = json.load(resp)
                choice = answer["choices"][0]
                msg = choice["message"]
                return (resp.status, msg["content"],
                        msg.get("reasoning_content"), msg.get("refusal"))

        with ThreadPoolExecutor(max_workers=8) as executor:
            observed = list(executor.map(call_seed, range(64)))
        failures = sum(seed % 7 == 0 for seed in range(64))
        check(all(code == 200 for code, *_ in observed),
              "SSE-only aggregate returned a non-200 JSON response")
        for seed, (code, content, reasoning, refusal) in enumerate(observed):
            failed = seed % 7 == 0
            declined = not failed and seed % 11 == 0
            expects_reasoning = not failed and not declined and seed % 13 == 0
            expected_content = "ok" if failed else None if declined else "stream-ok"
            check(content == expected_content,
                  f"SSE-only conversion/fallback corrupted seed {seed}: {content}")
            check(refusal == ("request declined" if declined else None),
                  f"SSE-only refusal text was lost for seed {seed}")
            check(reasoning == ("private reasoning" if expects_reasoning else None),
                  f"SSE-only reasoning text was lost for seed {seed}")

        upstream_attempts = upstream.calls
        upstream_streamed = upstream.streaming_seen
        live_fallback = live_stub.calls - baseline_live
        check(upstream_attempts == 64 and upstream_streamed == 64,
              f"SSE-only adapter did not request stream:true: "
              f"attempts={upstream_attempts} streaming={upstream_streamed}")
        check(live_fallback == failures,
              f"invalid upstream stream did not fail over: "
              f"fallback={live_fallback} expected={failures}")

        metrics_request = Request(
            f"http://127.0.0.1:{gateway_port}/metrics",
            headers={"Authorization":"Bearer only-for-local-test"})
        with urlopen(metrics_request, timeout=3) as response:
            raw = response.read(10 << 10).decode()
        metrics = {}
        for line in raw.splitlines():
            if line.startswith("guvna_"):
                name, _, value = line.partition(" ")
                metrics[name] = int(value)
        expected = {
            "guvna_requests_dispatched_total": 64,
            "guvna_upstream_attempts_total": 64 + failures,
            "guvna_upstream_success_total": 64,
            "guvna_upstream_invalid_responses_total": failures,
            "guvna_candidate_fallbacks_total": failures,
            "guvna_aggregation_completed_total": 64 - failures,
            "guvna_aggregation_busy_total": 0,
            "guvna_active_requests": 0,
        }
        for metric, want in expected.items():
            check(metrics.get(metric) == want,
                  f"SSE-only metric {metric}={metrics.get(metric)}, wanted {want}")

        # A request for more than 64 choices cannot be represented by
        # the bounded SSE synthesizer. A direct route has no JSON fallback:
        # the *actual Linux binary* must reject it without touching the
        # upstream at all or misclassifying it as an upstream failure.
        bad_choice_req = Request(
            api,
            data=json.dumps({"model":"streaming/only-stream",
                             "messages":[{"role":"user","content":"ping"}],
                             "n":65}).encode(),
            method="POST",
            headers={"Authorization":"Bearer only-for-local-test",
                     "Content-Type":"application/json"})
        try:
            with urlopen(bad_choice_req, timeout=3) as response:
                response.read()
                raise AssertionError("unsupported SSE-only n=65 returned 200")
        except HTTPError as error:
            payload = json.load(error)
            check(error.code == 422
                  and payload.get("error", {}).get("code") == "aggregate_n_unsupported",
                  f"SSE-only unsupported n returned wrong error: {error.code} {payload}")
        check(upstream.calls == upstream_attempts,
              "unsupported SSE aggregation consumed an unnecessary upstream HTTP attempt")
        usage = proc_info(proc.pid)
        check(usage["VmRSS"] <= 16 * 1024,
              "SSE-to-JSON adapter exceeds 16 MiB RSS in own release process")
        check(usage["fd"] <= 20, "SSE-to-JSON adapter leaked file descriptors")
        check(usage["Threads"] <= 12, "SSE-to-JSON adapter leaked threads")
        return {
            "passed": True, "requests": 64,
            "stream_only_attempts": upstream_attempts,
            "successful_aggregations": 64 - failures,
            "malformed_stream_fallbacks": failures,
            "rss_kib": usage["VmRSS"],
            "fd": usage["fd"], "threads": usage["Threads"],
        }
    finally:
        proc.terminate()
        try:
            _unused, captured = proc.communicate(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            _unused, captured = proc.communicate(timeout=5)
        upstream.shutdown()
        thread.join(timeout=3)
        text = captured.decode("utf-8", "replace")
        events = [line for line in text.splitlines() if line.startswith("{")]
        # A failed stream now emits both the overall failure event and one
        # fixed cause event. Each category has its own first-eight sampling
        # budget, so a large success wave cannot suppress rare faults.
        # Keep the gate strict: exactly 8 from every deterministic category,
        # no provider identifiers or unapproved fields, never just increase
        # the old event count ceiling to hide unexpected new logging.
        categories = {
            "aggregation_failed": [],
            "aggregation_completed": [],
            "aggregation_invalid_sse": [],
        }
        for line in events:
            record = json.loads(line)
            check(set(record) == {"ts_unix_ms", "level", "event", "sequence"},
                  "SSE-only local log contains unapproved metadata")
            check(record["event"] in categories,
                  f"unexpected aggregate diagnostic event: {record}")
            check(record["level"] in ("warn", "info"),
                  "unexpected local event severity")
            check(isinstance(record["ts_unix_ms"], int)
                  and record["ts_unix_ms"] > 0,
                  "invalid local diagnostic timestamp")
            categories[record["event"]].append(record["sequence"])
        for kind, sequences in categories.items():
            check(sorted(sequences) == list(range(1, 9)),
                  f"SSE-only log {kind} independently sampled unexpected sequences: {sequences}")
        check(len(events) == 24,
              f"SSE-only local log event cardinality changed: {len(events)}")
        check(all(token not in text for token in
                  ("fake-stream-key", "fake-live-key", "only-for-local-test",
                   "stream-ok", "request declined", "private reasoning", "fake-audio")),
              "SSE-only logs exposed credentials or generated content")


def verify_release_rate_limit_exhaustion(binary: str, root: str,
                                         env: dict, healthy_stub: Stub) -> dict:
    """Release-only recovery regression: 429-only exhaustion, local Auto,
    healthy fallback, read-only counters and privacy-safe local diagnostics."""
    limited = Stub("rate_limited")
    thread = threading.Thread(target=limited.serve_forever, daemon=True)
    thread.start()
    gateway_port = port()
    config = Path(root) / "rate-limit-recovery.yaml"
    config.write_text(f"""port: {gateway_port}
providers:
  - name: exhausted
    type: openai
    base_url: http://127.0.0.1:{limited.server_address[1]}
    key_env: RATE_TEST_KEY
    quarantine:
      rate_limit: 100ms
  - name: backup
    type: openai
    base_url: http://127.0.0.1:{healthy_stub.server_address[1]}
    key_env: LIVE_KEY
chains:
  - name: exhausted
    steps:
      - provider: exhausted
        model: simulated
  - name: with-fallback
    steps:
      - provider: exhausted
        model: simulated
      - provider: backup
        model: fast
auto:
  enabled: true
  candidates:
    - provider: exhausted
      model: simulated
      priority: 1
""", encoding="utf-8")
    extra_env = dict(env)
    extra_env["RATE_TEST_KEY"] = "local-only-synthetic-limited-key"
    extra_env["GUVNA_LOG_EVENTS"] = "1"
    rate_data = Path(root) / "rate-limit-data"
    proc = subprocess.Popen(
        [str(Path(binary).resolve()), "serve", "-config", str(config), "-data", str(rate_data)],
        stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, env=extra_env)
    try:
        health = f"http://127.0.0.1:{gateway_port}/healthz"
        started = time.monotonic()
        while time.monotonic() - started < 2:
            check(proc.poll() is None, "rate-limit gateway exited at startup")
            if get_status(health, timeout=0.1) == 200:
                break
            time.sleep(0.02)
        else:
            raise AssertionError("rate-limit gateway startup timeout")
        api = f"http://127.0.0.1:{gateway_port}/v1/chat/completions"
        auto_api = f"http://127.0.0.1:{gateway_port}/v1/auto"

        def call(model: str, use_auto: bool = False) -> tuple[int, int | None, str]:
            body = json.dumps({"model": model, "messages": [
                {"role": "user", "content": "diagnostic"}]}).encode("utf-8")
            req = Request(auto_api if use_auto else api, data=body, method="POST",
                          headers={"Authorization": "Bearer only-for-local-test",
                                   "Content-Type": "application/json"})
            try:
                with urlopen(req, timeout=5) as response:
                    code = response.status
                    retry = response.headers.get("Retry-After")
                    obj = json.load(response)
            except HTTPError as error:
                code = error.code
                retry = error.headers.get("Retry-After")
                obj = json.loads(error.read())
            detail = obj.get("error", {}).get("code", "") if code != 200 else (
                obj["choices"][0]["message"]["content"])
            return (code, int(retry) if retry is not None else None, detail)

        code, retry, detail = call("exhausted")
        check(code == 429 and detail == "providers_rate_limited",
              "initial 429 was not reported downstream")
        check(retry is not None and 3598 <= retry <= 3600,
              f"wrong bounded Retry-After: {retry}")
        check(limited.calls == 1, "initial throttling failed to quarantine")
        with ThreadPoolExecutor(max_workers=16) as executor:
            requests = list(executor.map(
                lambda i: call("auto" if i % 2 else "exhausted", bool(i % 2)),
                range(32)))
        check(all(status == 429 and detail == "providers_rate_limited"
                  and retry is not None and 3595 <= retry <= 3600
                  for status, retry, detail in requests),
              "Auto/Chat cooldown responses did not retain consistent retry hints")
        check(limited.calls == 1, "repeated clients bypassed quarantine")
        previous_healthy = healthy_stub.calls
        code, retry, detail = call("with-fallback")
        check(code == 200 and retry is None and detail == "ok",
              "healthy fallback was suppressed by unrelated rate limit")
        check(limited.calls == 1 and healthy_stub.calls == previous_healthy + 1,
              "fallback still touched quarantined credential")

        metrics_req = Request(f"http://127.0.0.1:{gateway_port}/metrics",
                              headers={"Authorization":"Bearer only-for-local-test"})
        with urlopen(metrics_req, timeout=3) as response:
            raw = response.read(12 << 10).decode("utf-8")
        metrics = {}
        for line in raw.splitlines():
            if line.startswith("guvna_"):
                name, _, value = line.partition(" ")
                metrics[name] = int(value)
        expected = {
            "guvna_requests_dispatched_total": 34,
            "guvna_upstream_attempts_total": 2,
            "guvna_upstream_success_total": 1,
            "guvna_credential_quarantines_total": 1,
            "guvna_upstream_http_failures_total": 1,
            "guvna_rate_limit_exhaustions_total": 33,
            "guvna_candidate_fallbacks_total": 1,
            "guvna_active_requests": 0,
        }
        for name, want in expected.items():
            check(metrics.get(name) == want,
                  f"rate-limit telemetry {name}={metrics.get(name)}, expected {want}")
        usage = proc_info(proc.pid)
        check(usage["VmRSS"] <= 16 * 1024,
              "rate-limit diagnostic subprocess idle RSS >16 MiB")
        check(usage["fd"] <= 20, "rate-limit diagnosis leaked file descriptors")
        check(usage["Threads"] <= 12, "rate-limit diagnosis leaked threads")
        return {"passed": True, "avoided_http_calls": 33,
                "downstream_429": 33, "fallback_200": 1,
                "metrics": expected, "rss_kib": usage["VmRSS"]}
    finally:
        proc.terminate()
        try:
            _, logs = proc.communicate(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            _, logs = proc.communicate(timeout=5)
        limited.shutdown()
        thread.join(timeout=3)
        log = logs.decode("utf-8", "replace")
        events = [json.loads(line) for line in log.splitlines()
                  if line.startswith("{")]
        check(any(e.get("event") == "rate_limit_exhausted" for e in events),
              "rate-limit failure class missing from local diagnostics")
        check(all(set(e) == {"ts_unix_ms", "level", "event", "sequence"}
                  for e in events), "local diagnostics used uncontrolled fields")
        check(all(secret not in log for secret in
                  ("local-only-synthetic-limited-key", "only-for-local-test",
                   "diagnostic", "simulated", "http://127.0.0.1:")),
              "rate-limit diagnostic log leaked content or internal endpoints")


def verify_release_chain_management(binary: str, config: Path, directory: str) -> dict:
    """Proof of local CLI persistence/recovery on the actual Linux release binary."""
    workspace = Path(directory) / "offline-chain-fixture"
    data = workspace / "data"
    workspace.mkdir()
    chain = workspace / "chain.yaml"
    chain.write_text("name: cli-fixture\nsteps:\n  - provider: live\n    model: first\n",
                     encoding="utf-8")
    binary = str(Path(binary).resolve())
    env = dict(os.environ)
    for name in ("API_KEYS", "ADMIN_KEY", "DOWN_KEY", "LIVE_KEY"):
        env.pop(name, None)

    def cli(*args: str, allowed: bool = True) -> str:
        process = subprocess.run(
            [binary, "chains", *args, "-config", str(config), "-data", str(data)],
            text=True, capture_output=True, env=env, timeout=5)
        if allowed:
            check(process.returncode == 0,
                  f"chains CLI failure {args}: {process.stderr}")
        else:
            check(process.returncode != 0,
                  f"chains CLI accepted invalid request {args}")
        return process.stdout if allowed else process.stderr

    original = json.loads(cli("list"))
    check(len(original) == 2, "CLI did not list static chains")
    check(not data.exists(), "read-only CLI created a data directory")
    check("created" in cli("set", "-file", str(chain)), "runtime set did not create")
    target = data / "chains.yaml"
    first = target.read_bytes()
    check(target.stat().st_mode & 0o777 == 0o600,
          "runtime chains file must be mode 0600 on Linux")
    listed = json.loads(cli("list"))
    check(len(listed) == 3 and listed[-1]["name"] == "cli-fixture",
          "CLI runtime list was not served from committed chains.yaml")

    chain.write_text("name: cli-fixture\nsteps:\n  - provider: live\n    model: second\n",
                     encoding="utf-8")
    check("updated" in cli("set", "-file", str(chain)), "runtime set did not update")
    check((data / "chains.yaml.bak").read_bytes() == first,
          "backup does not contain the prior committed version")
    second = target.read_bytes()
    check(b"second" in second, "second update not committed")
    check("deleted" in cli("delete", "-name", "cli-fixture"), "delete failed")

    corrupt = b"version: [\ninvalid corrupted on-disk file\n"
    target.write_bytes(corrupt)
    check("malformed" in cli("validate", allowed=False),
          "corrupt YAML was silently accepted")
    check("malformed" in cli("set", "-file", str(chain), allowed=False),
          "CLI silently rewrote corrupt live file")
    check(target.read_bytes() == corrupt, "invalid set modified damaged file")
    check("recovered 1 runtime" in cli("recover"), "recover failed")
    check(target.read_bytes() == second, "recover did not restore known-good backup")
    snapshots = list(data.glob("chains.yaml.before-recovery-*.bak"))
    check(len(snapshots) == 1 and snapshots[0].read_bytes() == corrupt,
          "damaged original was not preserved byte-for-byte")
    check(snapshots[0].stat().st_mode & 0o777 == 0o600,
          "forensic backup must not be world-readable")
    check("1 runtime" in cli("validate"), "CLI did not validate recovered file")
    check(target.stat().st_mode & 0o777 == 0o600,
          "recovered chains file lost mode 0600")

    def startup_rejected(expected: str) -> None:
        process = subprocess.run(
            [binary, "serve", "-config", str(config),
             "-data", str(data), "--healthcheck"],
            text=True, capture_output=True, env=env, timeout=3)
        check(process.returncode != 0 and expected in process.stderr,
              f"daemon startup did not reject unsafe runtime chains: "
              f"{expected}: {process.returncode}, {process.stderr}")

    # Validate actual Linux release startup, not only CLI. Duplicate and
    # shadowing runtime entries used to be quietly ignored or duplicated.
    invalid_runtime_cases = (
        ("version: 1\nchains:\n  - name: healthy\n"
         "    steps: [{provider: live, model: hijacked}]\n",
         "shadows config.yaml"),
        ("version: 1\nchains:\n  - name: repeated\n"
         "    steps: [{provider: live, model: one}]\n"
         "  - name: repeated\n"
         "    steps: [{provider: live, model: two}]\n",
         "duplicate runtime chain"),
        ("version: 999\nchains: []\n", "unsupported chains.yaml version"),
    )
    saved = target.read_bytes()
    rejected = 0
    for raw, expected in invalid_runtime_cases:
        target.write_text(raw, encoding="utf-8")
        startup_rejected(expected)
        check(expected in cli("validate", allowed=False),
              "manager and daemon gave different validation results")
        check(target.read_text(encoding="utf-8") == raw,
              "validation modified unsafe input")
        rejected += 1

    target.write_bytes(b"version: 1\nchains: []\n"
                       + b"#" * (1024 * 1024))
    startup_rejected("at most 1 MiB")
    check("at most 1 MiB" in cli("validate", allowed=False),
          "manager accepted oversized runtime file")
    rejected += 1

    # Linux open(O_NOFOLLOW) must reject a symlink, even if its target is
    # well-formed. The target is not touched.
    target.unlink()
    exterior = workspace / "external-chains.yaml"
    exterior.write_bytes(saved)
    target.symlink_to(exterior)
    startup_rejected("regular file")
    check("regular file" in cli("validate", allowed=False),
          "manager followed runtime symlink")
    check(exterior.read_bytes() == saved, "symlink target was modified")
    rejected += 1
    target.unlink()
    target.write_bytes(saved)

    return {"passed": True, "static_routes": 2, "recovered_routes": 1,
            "startup_rejection_cases": rejected,
            "corrupt_bytes_preserved": len(corrupt),
            "atomic_backup_matches_prior": True, "mode": "0600"}


def verify_release_auto_and_local_logs(binary: str, cfg: Path, root: str,
                                       env: dict, mock1: Stub, mock2: Stub) -> dict:
    """64 real Linux release requests: Auto ranking, failure fallback,
    diagnostic privacy, bounded resources. Separate process avoids changing
    the fixed existing baseline metrics and fault accounting."""
    auto_port = port()
    auto_cfg = Path(root) / "auto-enabled.yaml"
    original = cfg.read_text(encoding="utf-8")
    original_port = original.splitlines()[0]
    check(original_port.startswith("port: "), "unexpected quality YAML port")
    changed = original.replace(original_port, f"port: {auto_port}", 1)
    changed += ("""
auto:
  enabled: true
  candidates:
    - provider: down
      model: down
      priority: 1
      max_input_bytes: 256
    - provider: live
      model: fast
      priority: 2
""")
    auto_cfg.write_text(changed, encoding="utf-8")
    autodata = Path(root) / "auto-data"
    autodata.mkdir()
    auto_env = dict(env, GUVNA_LOG_EVENTS="1", GUVNA_TELEMETRY_SQLITE="1")
    proc = subprocess.Popen(
        [str(Path(binary).resolve()), "serve", "-config", str(auto_cfg),
         "-data", str(autodata)],
        stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, env=auto_env)
    log_content = ""
    try:
        started = time.monotonic()
        health = f"http://127.0.0.1:{auto_port}/healthz"
        while time.monotonic() - started < 2:
            check(proc.poll() is None, "Auto process exited at startup")
            if get_status(health, timeout=0.15) == 200:
                break
            time.sleep(0.02)
        else:
            raise AssertionError("Auto process startup timed out")
        primary_before, healthy_before = mock1.calls, mock2.calls
        wave = run_wave(
            f"http://127.0.0.1:{auto_port}/v1/auto", "auto", 64, 8, proc.pid)
        primary = mock1.calls - primary_before
        healthy = mock2.calls - healthy_before
        check(primary == 64 and healthy == 64,
              f"Auto failed shared fallback: primary={primary}, healthy={healthy}")
        metrics_req = Request(
            f"http://127.0.0.1:{auto_port}/metrics",
            headers={"Authorization": "Bearer only-for-local-test"})
        with urlopen(metrics_req, timeout=3) as resp:
            raw = resp.read(8192).decode("utf-8")
        for key, expected in [
            ("guvna_requests_dispatched", 64),
            ("guvna_upstream_attempts", 128),
            ("guvna_candidate_fallbacks", 64),
            ("guvna_upstream_success", 64),
        ]:
            lines = [line for line in raw.splitlines()
                     if line.startswith(key + "_total ")]
            check(len(lines) == 1 and int(lines[0].split()[1]) == expected,
                  f"Auto metrics {key} != {expected}: {lines}")
        telemetry_deadline = time.monotonic() + 3
        telemetry_written = telemetry_dropped = telemetry_write_failures = 0
        while time.monotonic() < telemetry_deadline:
            telemetry_req = Request(
                f"http://127.0.0.1:{auto_port}/metrics",
                headers={"Authorization": "Bearer only-for-local-test"})
            with urlopen(telemetry_req, timeout=1) as resp:
                telemetry_raw = resp.read(8192).decode("utf-8")
            telemetry_values = {}
            for line in telemetry_raw.splitlines():
                if line.startswith("guvna_telemetry_"):
                    name, _, value = line.partition(" ")
                    telemetry_values[name] = int(value)
            telemetry_written = telemetry_values.get("guvna_telemetry_written_total", 0)
            telemetry_dropped = telemetry_values.get("guvna_telemetry_dropped_total", 0)
            telemetry_write_failures = telemetry_values.get("guvna_telemetry_write_failures_total", 0)
            if telemetry_written >= 128 or telemetry_dropped or telemetry_write_failures:
                break
            time.sleep(0.02)
        check(telemetry_written >= 128,
              f"SQLite telemetry did not persist 128 attempt/request events: {telemetry_written}")
        check(telemetry_dropped == 0 and telemetry_write_failures == 0,
              f"SQLite telemetry dropped/wrote-failed events: {telemetry_dropped}/{telemetry_write_failures}")

        db_path = autodata / "telemetry" / "requests.sqlite3"
        check(db_path.is_file(), "opt-in telemetry database was not created")
        db_uri = db_path.resolve().as_uri() + "?mode=ro"
        db = sqlite3.connect(db_uri, uri=True, timeout=1)
        try:
            request_rows = db.execute("SELECT COUNT(*) FROM request_events").fetchone()[0]
            failed_candidates = db.execute(
                "SELECT COUNT(*) FROM failure_events WHERE provider='down' AND model='down' "
                "AND status=503 AND failure_type='provider_5xx'").fetchone()[0]
            down_health = db.execute(
                "SELECT penalty,consecutive_failures,demoted FROM provider_health "
                "WHERE provider='down' AND model='*'").fetchone()
            down_hourly = db.execute(
                "SELECT request_count,failure_count FROM hourly_stats "
                "WHERE provider='down' AND model='down' AND failure_type='provider_5xx'").fetchone()
            journal_mode = db.execute("PRAGMA journal_mode").fetchone()[0].lower()
            max_pages = db.execute("PRAGMA max_page_count").fetchone()[0]
            columns = {row[1].lower() for row in db.execute("PRAGMA table_info(request_events)")}
        finally:
            db.close()
        check(request_rows == 64, f"SQLite request rows={request_rows}, expected 64")
        check(failed_candidates == 64,
              f"fallback provider failures were not retained: {failed_candidates}")
        check(down_hourly == (64, 64),
              f"hourly supplier attempt/failure totals incorrect: {down_hourly}")
        check(down_health is not None and down_health[0] == 100
              and down_health[1] >= 3 and down_health[2] == 1,
              f"provider health history was not persisted: {down_health}")
        check(journal_mode == "wal", f"telemetry database journal mode={journal_mode}")
        check(max_pages <= 8192, f"telemetry SQLite page cap unexpectedly grew: {max_pages}")
        forbidden = {"prompt", "body", "request_body", "api_key", "authorization", "headers"}
        check(not columns.intersection(forbidden),
              f"telemetry schema contains sensitive payload columns: {columns.intersection(forbidden)}")
        db_size = sum(path.stat().st_size for path in (
            db_path, Path(str(db_path) + "-wal"), Path(str(db_path) + "-shm")
        ) if path.exists())
        check(db_size < 32 * 1024 * 1024,
              f"telemetry database and WAL exceeded 32 MiB: {db_size}")

        usage = proc_info(proc.pid)
        check(usage["VmRSS"] <= 16 * 1024, "Auto process RSS exceeded 16 MiB")
        check(usage["fd"] <= 20, "Auto process leaked file descriptors")
        check(usage["Threads"] <= 12, "Auto process leaked threads")
        return {
            "passed": True, "wave": wave,
            "primary_attempts": primary, "fallback_successes": healthy,
            "telemetry_written": telemetry_written,
            "telemetry_dropped": telemetry_dropped,
            "telemetry_write_failures": telemetry_write_failures,
            "sqlite_request_rows": request_rows,
            "sqlite_fallback_failure_rows": failed_candidates,
            "sqlite_down_provider_hourly": {"attempts": down_hourly[0], "failures": down_hourly[1]},
            "sqlite_down_provider_health": {"penalty": down_health[0], "consecutive_failures": down_health[1], "demoted": bool(down_health[2])},
            "sqlite_bytes_with_wal": db_size,
            "rss_kib": usage["VmRSS"], "fds": usage["fd"],
            "threads": usage["Threads"]
        }
    finally:
        proc.terminate()
        try:
            _stdout, captured = proc.communicate(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            _stdout, captured = proc.communicate(timeout=5)
        log_content = captured.decode("utf-8", "replace")
        events = [line for line in log_content.splitlines() if line.startswith("{")]
        check(1 <= len(events) <= 10,
              f"opt-in local warning logs must be bounded: {len(events)}")
        for raw_event in events:
            event = json.loads(raw_event)
            check(set(event) == {"ts_unix_ms", "level", "event", "sequence"},
                  "local warning includes unapproved telemetry fields")
            check(event["event"] in {"provider_http_failure", "transport_failure",
                                    "upstream_invalid", "stream_preflight_failure",
                                    "provider_circuit_open"},
                  "local warning has unapproved event label")
        check("fake-down-key" not in log_content
              and "fake-live-key" not in log_content
              and "only-for-local-test" not in log_content,
              "local logs exposed test credential")

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


def verify_structured_output_on_release(endpoint: str, healthy_stub: Stub) -> dict:
    """Public Responses handler -> shared dispatch -> real mock HTTP upstream."""
    def submit(payload: dict, *, streaming=False) -> tuple[int, str]:
        body = json.dumps(payload, separators=(",", ":")).encode()
        request = Request(endpoint, data=body, method="POST", headers={
            "Authorization": "Bearer only-for-local-test",
            "Content-Type": "application/json",
        })
        try:
            with urlopen(request, timeout=8) as reply:
                return reply.status, reply.read().decode()
        except HTTPError as err:
            return err.code, err.read().decode()

    before = healthy_stub.calls
    common = {"model": "healthy", "input": "hello"}
    invalid = dict(common, text={"format":{"type":"json_schema",
                        "name":"bad name","schema":{"type":"object"}}})
    code, raw = submit(invalid)
    check(code == 400 and json.loads(raw)["error"]["code"] == "invalid_text_format",
          f"Release accepted invalid structured format: {code}, {raw}")
    check(healthy_stub.calls == before, "invalid format contacted upstream")

    schema = {"type":"json_schema","name":"shape_1","strict":True,
              "schema":{"type":"object","properties":{
                  "big":{"const":12345678901234567890}}}}
    for format in ({"type":"json_object"}, schema):
        code, raw = submit(dict(common, text={"format":format}))
        check(code == 200, f"Release failed structured output request: {code}, {raw}")
        result = json.loads(raw)
        check(result["status"] == "completed" and result["output_text"] == '{"ok":true}',
              f"Release corrupted JSON output: {result}")

        code, raw = submit(dict(common, stream=True, text={"format":format}), streaming=True)
        check(code == 200, f"Release failed structured SSE request: {code}, {raw}")
        check(raw.count("event: response.completed") == 1 and
              "event: response.failed" not in raw,
              f"Release lost JSON structured SSE terminal state: {raw[:1200]}")
        check('"output_text":"{\\"ok\\":true}"' in raw or
              '"output_text": "{\\"ok\\":true}"' in raw,
              "Release structured output text missing from SSE")

    # A 200 from the provider is *not* successful if JSON output was ignored.
    code, raw = submit(dict(common, input="badjson",
                            text={"format":{"type":"json_object"}}))
    check(code == 502 and json.loads(raw)["error"]["code"] == "upstream_unavailable",
          f"ignored JSON mode was reported as completed: {code}, {raw}")
    code, raw = submit(dict(common, input="badjson", stream=True,
                            text={"format":{"type":"json_object"}}), streaming=True)
    check(code == 200 and raw.count("event: response.failed") == 1
          and "event: response.completed" not in raw,
          f"ignored streaming JSON mode claimed completed: {code}, {raw[:1200]}")

    with healthy_stub.lock:
        observed = list(healthy_stub.structured)
        count = healthy_stub.calls
    check(len(observed) == 6 and count == before + 6,
          f"structured requests lost or retried: observed={len(observed)} calls={count-before}")
    check(observed[0] == {"type":"json_object"} and observed[1] == {"type":"json_object"},
          "Chat JSON object response_format mapping differs from contract")
    mapped = {"type":"json_schema", "json_schema":{
        "name":"shape_1","strict":True,"schema":schema["schema"]}}
    check(observed[2] == mapped and observed[3] == mapped,
          f"schema fields lost or numbers rounded: {observed[2]}")
    return {"passed": True, "release_requests": 7,
            "rejected_pre_network": 1, "structured_upstream_attempts": 6,
            "json_number_preserved": True, "stream_fail_closed": True}


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
            summary["release_chain_cli_gate"] = verify_release_chain_management(
                a.binary, cfg, directory)
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
                # Exercise streaming Responses on the real release binary, not only unit mocks.
                stream_requests = min(200, max(30, a.requests // 10))
                stream_clients = min(16, a.clients)
                response_endpoint = f"http://127.0.0.1:{gateway_port}/v1/responses"
                summary["responses_sse_wave"] = run_responses_sse_wave(
                    response_endpoint, stream_requests, stream_clients, proc.pid)
                summary["post_sse_idle"] = idle(proc.pid, a.idle_seconds)
                check(summary["responses_sse_wave"]["peak_rss_kib"] <=
                      a.max_peak_rss_mib * 1024, "Responses SSE peak RSS limit exceeded")
                check(summary["post_sse_idle"]["rss_kib_max"] <=
                      a.max_idle_rss_mib * 1024, "post Responses SSE idle RSS exceeded")
                check(summary["post_sse_idle"]["fds"] - summary["post_wave2"]["fds"] <= 8,
                      "Responses SSE leaked file descriptors")
                check(summary["post_sse_idle"]["threads"] - summary["post_wave2"]["threads"] <= 8,
                      "Responses SSE leaked threads")
                summary["metrics"] = verify_metrics(
                    f"http://127.0.0.1:{gateway_port}/metrics",
                    a.requests, stream_requests)
                check(mock1.calls == a.requests, f"chaos attempt count {mock1.calls} != {a.requests}")
                check(mock2.calls == a.requests * 2 + stream_requests,
                      f"healthy provider call count {mock2.calls} != {a.requests * 2 + stream_requests}")
                summary["upstream_calls"] = {"primary": mock1.calls, "healthy": mock2.calls}
                summary["faults"] = mock1.faults
                check(all(count > 0 for count in mock1.faults.values()),
                      "fault injection did not cover every variant")
                summary["sse_only_json_gate"] = verify_release_stream_only_aggregation(
                    a.binary, directory, env, mock2)
                summary["opt_in_circuit_gate"] = exercise_opt_in_circuit_gate(
                    a.binary, cfg, directory, env, mock1, mock2)
                summary["rate_limit_recovery_gate"] = verify_release_rate_limit_exhaustion(
                    a.binary, directory, env, mock2)
                summary["auto_and_local_log_gate"] = verify_release_auto_and_local_logs(
                    a.binary, cfg, directory, env, mock1, mock2)
                summary["structured_output_gate"] = verify_structured_output_on_release(
                    response_endpoint, mock2)
                summary["post_structured_idle"] = idle(proc.pid, a.idle_seconds)
                check(summary["post_structured_idle"]["rss_kib_max"] <=
                      a.max_idle_rss_mib * 1024, "structured output idle RSS exceeded")
                check(summary["post_structured_idle"]["fds"] -
                      summary["post_sse_idle"]["fds"] <= 8,
                      "structured output handler leaked file descriptors")
                check(summary["post_structured_idle"]["threads"] -
                      summary["post_sse_idle"]["threads"] <= 8,
                      "structured output handler leaked threads")
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
