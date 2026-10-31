#!/usr/bin/env python3
"""Same-host Release A/B, serial dual-protocol traffic and concurrency ladder.

Reports gateway + loopback HTTP latency, not model generation latency or VPS
performance. Standard 16/48/6 MiB resource limits remain in quality_gate.py.
"""
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import subprocess
import tempfile
import threading
import time
from urllib.request import Request, build_opener, ProxyHandler
from urllib.error import HTTPError

from quality_gate import port, proc_info, idle, check


class Stub(ThreadingHTTPServer):
    daemon_threads = True
    request_queue_size = 256


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *_args):
        pass

    def do_POST(self):
        query = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
        with self.server.lock:
            self.server.calls += 1
            number = self.server.calls
        if self.path == '/v1/responses':
            result = {'id': f'resp_{number}', 'object': 'response', 'model': query['model'],
                      'created_at': 123, 'status': 'completed', 'error': None,
                      'output': [{'type': 'message', 'role': 'assistant', 'content': [
                          {'type': 'output_text', 'text': 'ok', 'annotations': []}]}]}
        else:
            result = {'id': f'chat_{number}', 'object': 'chat.completion', 'model': query['model'],
                      'created': 123, 'choices': [{'index': 0, 'message': {
                          'role': 'assistant', 'content': 'ok'}, 'finish_reason': 'stop'}]}
        payload = json.dumps(result).encode()
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)


def query(base, lane, number=0, *, stored=False, size=0):
    if lane == 'chat':
        path, value = '/chat/completions', {'model': 'demo', 'messages': [{'role': 'user', 'content': 'ping'}]}
    elif lane == 'auto':
        path, value = '/responses', {'model': 'auto', 'input': 'ping', 'store': False}
    else:
        path, value = '/responses', {'model': 'native' if lane == 'native' else 'demo',
                                   'input': 'x' * size if size else 'ping', 'store': stored}
    started = time.perf_counter()
    request = Request(base + path, data=json.dumps(value).encode(), headers={
        'Authorization': 'Bearer only-for-local-test', 'Content-Type': 'application/json'})
    try:
        with build_opener(ProxyHandler({})).open(request, timeout=15) as reply:
            result = json.load(reply)
            content = (result['choices'][0]['message']['content'] if lane == 'chat'
                       else result['output'][0]['content'][0]['text'])
            return reply.status, content, (time.perf_counter() - started) * 1000
    except HTTPError as error:
        return error.code, 'http_error', (time.perf_counter() - started) * 1000


def wave(base, pid, lane, clients, count, *, mixed=False, stored=False):
    latencies, statuses, rss = [], {}, []
    start = time.monotonic()
    cpu_before = proc_info(pid)['cpu_ticks']
    with ThreadPoolExecutor(max_workers=clients) as pool:
        futures = [pool.submit(query, base, ['chat', 'responses', 'native', 'auto'][i % 4]
                               if mixed else lane, i, stored=stored) for i in range(count)]
        for future in as_completed(futures):
            status, content, latency = future.result()
            label = f'{status}:{content}'
            statuses[label] = statuses.get(label, 0) + 1
            latencies.append(latency)
            rss.append(proc_info(pid)['VmRSS'])
    elapsed = time.monotonic() - start
    cpu_after = proc_info(pid)['cpu_ticks']
    check(statuses == {'200:ok': count}, f'{lane} returned {statuses}')
    latencies.sort()
    percentile = lambda q: round(latencies[int((len(latencies)-1) * q)], 3)
    return {'lane': lane, 'clients': clients, 'requests': count, 'seconds': round(elapsed, 3),
            'p50_ms': percentile(.5), 'p95_ms': percentile(.95), 'p99_ms': percentile(.99),
            'cpu_percent_single_core': round(100 * (cpu_after-cpu_before) / os.sysconf('SC_CLK_TCK') / elapsed, 3),
            'rss_kib_max': max(rss), 'statuses': statuses}


def measure(binary, upstream, *, full):
    result = {'binary': Path(binary).name}
    with tempfile.TemporaryDirectory(prefix='guvna-lowload-') as directory:
        listen = port()
        config = Path(directory) / 'config.yaml'
        config.write_text(f'''port: {listen}
providers:
  - name: chat
    type: openai
    base_url: {upstream}
    key_env: TEST_KEY
  - name: native
    type: openai
    base_url: {upstream}
    key_env: TEST_KEY
    responses_protocol: native
chains:
  - name: demo
    steps:
      - provider: chat
        model: physical-chat
  - name: native
    steps:
      - provider: native
        model: physical-native
auto:
  enabled: true
  candidates:
    - provider: chat
      model: physical-chat
    - provider: native
      model: physical-native
''', encoding='utf-8')
        env = dict(os.environ, API_KEYS='only-for-local-test', TEST_KEY='local-mock-key', GUVNA_BIND_ADDR='127.0.0.1')
        started = time.monotonic()
        process = subprocess.Popen([str(Path(binary).resolve()), 'serve', '-config', str(config), '-data', directory],
                                   env=env, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
        try:
            base = f'http://127.0.0.1:{listen}/v1'
            while time.monotonic() - started < 5:
                check(process.poll() is None, 'gateway exited before readiness')
                try:
                    if query(base, 'chat')[0] == 200:
                        break
                except OSError:
                    time.sleep(.02)
            else:
                raise AssertionError('gateway did not become ready')
            result['startup_seconds'] = round(time.monotonic() - started, 3)
            result['idle_before'] = idle(process.pid, 3)
            result['serial'] = [wave(base, process.pid, lane, 1, 100) for lane in ['chat', 'responses', 'native', 'auto']]
            if full:
                result['ladder'] = [wave(base, process.pid, 'mixed', level, 200, mixed=True)
                                    for level in [1, 2, 4, 8, 16, 64]]
                result['aging'] = wave(base, process.pid, 'mixed', 16, 2000, mixed=True)
                result['state_flood'] = wave(base, process.pid, 'native', 8, 1200, stored=True)
                result['one_mib_input'] = query(base, 'native', size=1024*1024)[0]
                check(result['one_mib_input'] == 200, '1 MiB native input was rejected')
            result['idle_after'] = idle(process.pid, 3)
            result['rss_growth_kib'] = result['idle_after']['rss_kib_last'] - result['idle_before']['rss_kib_last']
            result['fd_growth'] = result['idle_after']['fds'] - result['idle_before']['fds']
            result['threads_growth'] = result['idle_after']['threads'] - result['idle_before']['threads']
            check(result['fd_growth'] <= 8 and result['threads_growth'] <= 8, 'FD/thread growth exceeded existing limits')
            check(result['rss_growth_kib'] <= 6*1024, 'idle RSS growth exceeded existing 6 MiB limit')
            check(result['idle_after']['cpu_percent_single_core'] <= 1, 'idle CPU exceeded existing 1% limit')
        finally:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
            process.stderr.close()
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--binary', required=True)
    parser.add_argument('--baseline')
    parser.add_argument('--out', default='lowload-results.json')
    args = parser.parse_args()
    summary = {'passed': False, 'scope': 'local Linux Release HTTP mock; no VPS or live supplier evidence'}
    server = Stub(('127.0.0.1', 0), Handler)
    server.daemon_threads = True
    server.request_queue_size = 256
    server.lock, server.calls = threading.Lock(), 0
    threading.Thread(target=server.serve_forever, daemon=True).start()
    upstream = f'http://127.0.0.1:{server.server_address[1]}'
    try:
        if args.baseline:
            summary['baseline'] = measure(args.baseline, upstream, full=False)
        summary['candidate'] = measure(args.binary, upstream, full=True)
        if args.baseline:
            summary['p95_ratios'] = {after['lane']: round(after['p95_ms']/before['p95_ms'], 3)
                                     for before, after in zip(summary['baseline']['serial'], summary['candidate']['serial'])}
        summary['passed'] = True
    except Exception as error:
        summary['error'] = str(error)
        raise
    finally:
        server.shutdown()
        server.server_close()
        Path(args.out).write_text(json.dumps(summary, indent=2)+'\n', encoding='utf-8')
    print(json.dumps(summary))


if __name__ == '__main__':
    main()
