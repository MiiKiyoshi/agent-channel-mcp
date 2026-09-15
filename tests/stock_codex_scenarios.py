"""The delivery against the installed Codex, isolated: a temporary CODEX_HOME with a
loopback mock model provider, a stock `codex app-server --listen stdio://` hosting the
receiving thread (as the Codex TUI does), and the waiter's own app-server child writing
the shared queue. Run under `unshare -rn` with loopback up, so nothing leaves the host.
Writes one JSON object to the path given: per scenario, how many user messages carry
the message's key at the thread, and how often the message text reached the model.

Scenarios: the answer to an add is lost; the message was consumed before the retry;
the waiter dies between the add and the acknowledgement; two waiters retry at once;
the key is held by other text.
"""

import json
import os
import signal
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from agent_channel_mcp import waiter as waiter_module          # noqa: E402
from agent_channel_mcp.codex_sink import CodexSink, KeyConflict  # noqa: E402
from agent_channel_mcp.store import Store                       # noqa: E402
from agent_channel_mcp.waiter import DeliveryUncertain, WaiterSignal, deliver_to_codex, render_message  # noqa: E402

requests_seen: list[dict] = []


class MockProvider(BaseHTTPRequestHandler):
    """Answers every Responses API call with the assistant message "ok"."""

    def log_message(self, *args):
        pass

    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0)) or 0) or b"{}")
        requests_seen.append(body)
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.end_headers()
        item = {"type": "message", "id": "msg_1", "role": "assistant", "status": "completed",
                "content": [{"type": "output_text", "text": "ok", "annotations": []}]}
        events = [
            ("response.created", {"response": {"id": "resp_1", "status": "in_progress", "output": []}}),
            ("response.output_item.added", {"output_index": 0, "item": {**item, "status": "in_progress", "content": []}}),
            ("response.output_text.delta", {"output_index": 0, "content_index": 0, "item_id": "msg_1", "delta": "ok"}),
            ("response.output_item.done", {"output_index": 0, "item": item}),
            ("response.completed", {"response": {"id": "resp_1", "status": "completed", "output": [item],
                                                 "usage": {"input_tokens": 1, "output_tokens": 1, "total_tokens": 2}}}),
        ]
        for name, data in events:
            self.wfile.write(f"event: {name}\ndata: {json.dumps({'type': name, **data})}\n\n".encode())
        self.wfile.flush()


def user_texts(request: dict) -> list[str]:
    return [part.get("text", "") for message in request.get("input", [])
            if isinstance(message, dict) and message.get("role") == "user"
            for part in (message.get("content") or []) if isinstance(part, dict)]


class Host:
    """A stock app-server on stdio that hosts threads, with its notifications drained."""

    def __init__(self, home: Path):
        self.process = subprocess.Popen(
            ["codex", "app-server", "--listen", "stdio://"], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=open(home / "host-stderr.log", "a"), text=True,
        )
        self.lock = threading.Lock()
        self.next_id = 0
        self.answers: dict[int, dict] = {}
        self.turns_completed: dict[str, int] = {}
        self.changed = threading.Condition()
        threading.Thread(target=self._reader, daemon=True).start()
        self.call("initialize", {"clientInfo": {"name": "stock-codex-scenarios", "version": "0"},
                                 "capabilities": {"experimentalApi": True}})
        self._write({"jsonrpc": "2.0", "method": "initialized", "params": {}})

    def _write(self, message: dict) -> None:
        with self.lock:
            self.process.stdin.write(json.dumps(message) + "\n")
            self.process.stdin.flush()

    def _reader(self) -> None:
        for line in self.process.stdout:
            try:
                message = json.loads(line)
            except json.JSONDecodeError:
                continue
            with self.changed:
                if "method" in message and "id" in message:
                    self._write({"jsonrpc": "2.0", "id": message["id"],
                                 "error": {"code": -32601, "message": "not handled"}})
                elif "method" in message:
                    if message["method"] == "turn/completed":
                        thread_id = message["params"]["threadId"]
                        self.turns_completed[thread_id] = self.turns_completed.get(thread_id, 0) + 1
                else:
                    self.answers[message["id"]] = message
                self.changed.notify_all()

    def call(self, method: str, params: dict | None = None, timeout: float = 90) -> dict:
        with self.changed:
            self.next_id += 1
            request_id = self.next_id
        self._write({"jsonrpc": "2.0", "id": request_id, "method": method, "params": params or {}})
        deadline = time.monotonic() + timeout
        with self.changed:
            while request_id not in self.answers:
                if not self.changed.wait(timeout=max(0.0, deadline - time.monotonic())):
                    raise TimeoutError(method)
            message = self.answers.pop(request_id)
        if "error" in message:
            raise RuntimeError(f"{method}: {message['error']}")
        return message.get("result") or {}

    def wait_turns(self, thread_id: str, count: int, timeout: float = 90) -> None:
        deadline = time.monotonic() + timeout
        with self.changed:
            while self.turns_completed.get(thread_id, 0) < count:
                if not self.changed.wait(timeout=max(0.0, deadline - time.monotonic())):
                    raise TimeoutError(f"turn {count} on {thread_id}")

    def new_thread(self, home: Path) -> str:
        """A thread with one completed turn, as any live thread has had."""
        thread_id = self.call("thread/start", {"cwd": str(home), "model": "mock-1", "modelProvider": "mock"})["thread"]["id"]
        self.call("turn/start", {"threadId": thread_id, "input": [{"type": "text", "text": "hello"}]})
        self.wait_turns(thread_id, 1)
        return thread_id

    def stop(self) -> None:
        self.process.terminate()
        try:
            self.process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            self.process.kill()


class Scenario:
    """One receiving thread, one channel message, and the reads that judge the outcome."""

    def __init__(self, host: Host, home: Path, name: str):
        self.host = host
        self.name = name
        self.thread_id = host.new_thread(home)
        self.db = home / f"{name}.sqlite3"
        self.store = Store(self.db)
        sender = self.store.participant("room", "plan")
        self.receiver = self.store.participant("room", "exec")
        self.store.activate(self.receiver["id"], "tok")
        self.message_id = self.store.send(sender["id"], f"message for {name}", to="exec")[0]["message_id"]
        self.message = self.store.pending_for_token("tok")
        self.text = render_message(self.message)
        self.key = f"agent-channel:{self.store.channel_id}:{self.message_id}"
        self.requests_before = len(requests_seen)

    def run_waiter(self, store_class=Store, timeout: float = 60) -> int | None:
        """The real waiter loop, in a thread, until the message is acknowledged, a
        conflict is on record, or the time is up."""
        stop = threading.Event()
        result: list = []
        original = waiter_module.Store
        waiter_module.Store = store_class
        try:
            thread = threading.Thread(
                target=lambda: result.append(waiter_module.run(self.db, "tok", self.thread_id, stop)), daemon=True)
            thread.start()
            deadline = time.monotonic() + timeout
            while time.monotonic() < deadline and thread.is_alive():
                run = self.store.last_waiter(self.receiver["id"], "tok")
                if self.store.pending(self.receiver["id"]) is None:
                    break
                if run is not None and run["last_error"] and "key conflict" in run["last_error"]:
                    break
                time.sleep(0.1)
            stop.set()
            thread.join(timeout=15)
        finally:
            waiter_module.Store = original
        return result[0] if result else None

    def wait_consumed(self, turns: int, timeout: float = 60) -> None:
        """Until the host has completed `turns` turns on the thread and its queue is empty."""
        self.host.wait_turns(self.thread_id, turns, timeout)
        deadline = time.monotonic() + timeout
        while self.host.call("thread/queue/list", {"threadId": self.thread_id})["data"]:
            if time.monotonic() > deadline:
                raise TimeoutError("queue not drained")
            time.sleep(0.5)

    def outcome(self) -> dict:
        items = []
        cursor = None
        while True:
            page = self.host.call("thread/items/list", {"threadId": self.thread_id, "cursor": cursor})
            items.extend(entry.get("item", entry) for entry in page["data"])
            cursor = page.get("nextCursor")
            if not cursor:
                break
        with_key = [item for item in items if item.get("type") == "userMessage" and item.get("clientId") == self.key]
        last = requests_seen[-1] if len(requests_seen) > self.requests_before else {}
        run = self.store.last_waiter(self.receiver["id"], "tok")
        return {
            "items_with_key": len(with_key),
            "texts_with_key": ["".join(p.get("text", "") for p in item.get("content", [])) for item in with_key],
            "model_inputs_with_text": sum(1 for text in user_texts(last) if text == self.text),
            "model_requests": len(requests_seen) - self.requests_before,
            "queue": self.host.call("thread/queue/list", {"threadId": self.thread_id})["data"],
            "acknowledged": self.store.pending(self.receiver["id"]) is None,
            "last_error": run["last_error"] if run else None,
        }


def accepted_response_lost(scenario: Scenario) -> dict:
    """The waiter marked the message and the app-server took it, but the answer never
    came back (the state a timeout or a crash leaves): the next attempt must find it."""
    scenario.store.mark_attempted(scenario.message_id)
    sink = CodexSink(timeout=60)
    sink.add(scenario.thread_id, scenario.key, scenario.text)
    sink.close()
    scenario.run_waiter()
    scenario.wait_consumed(turns=2)
    return scenario.outcome()


def consumed_before_retry(scenario: Scenario) -> dict:
    scenario.store.mark_attempted(scenario.message_id)
    sink = CodexSink(timeout=60)
    sink.add(scenario.thread_id, scenario.key, scenario.text)
    sink.close()
    scenario.wait_consumed(turns=2)
    scenario.run_waiter()
    return scenario.outcome()


def waiter_dies_before_the_ack(scenario: Scenario) -> dict:
    """A signal lands between the add and the acknowledgement; the process ends the
    way a signalled waiter does, and a fresh one takes over."""

    class SignalledAtAck(Store):
        def ack_for_token(self, token, message_id):
            raise WaiterSignal(signal.SIGTERM)

    exit_code = scenario.run_waiter(SignalledAtAck)
    assert exit_code == 128 + signal.SIGTERM, exit_code
    assert scenario.store.pending(scenario.receiver["id"]) is not None
    scenario.run_waiter()
    scenario.wait_consumed(turns=2)
    return scenario.outcome()


def concurrent_retry(scenario: Scenario) -> dict:
    """Two waiters (an old one and its takeover) deliver the same message at once, each
    having read it unmarked."""
    message = scenario.store.pending_for_token("tok")

    def deliver(_):
        own = Store(scenario.db)
        sink = CodexSink(timeout=60)
        try:
            return deliver_to_codex(own, sink, scenario.thread_id, message, scenario.text)
        except DeliveryUncertain:
            return "uncertain"
        finally:
            sink.close()
            own.close()

    with ThreadPoolExecutor(2) as pool:
        results = sorted(pool.map(deliver, range(2)))
    scenario.wait_consumed(turns=2)
    return {**scenario.outcome(), "results": results}


def body_conflict(scenario: Scenario) -> dict:
    """The key is already held by other text: reported, never added, never acknowledged."""
    scenario.store.mark_attempted(scenario.message_id)
    sink = CodexSink(timeout=60)
    sink.add(scenario.thread_id, scenario.key, "other text under the same key")
    sink.close()
    scenario.wait_consumed(turns=2)
    scenario.run_waiter()
    conflict = None
    try:
        probe = CodexSink(timeout=60)
        try:
            probe.find(scenario.thread_id, scenario.key, scenario.text)
        finally:
            probe.close()
    except KeyConflict as error:
        conflict = str(error)
    time.sleep(2)
    return {**scenario.outcome(), "conflict": conflict}


SCENARIOS = [accepted_response_lost, consumed_before_retry, waiter_dies_before_the_ack, concurrent_retry, body_conflict]


def main(output: Path) -> None:
    home = Path(tempfile.mkdtemp(prefix="agent-channel-stock-codex-", dir="/dev/shm" if os.path.isdir("/dev/shm") else None))
    mock = HTTPServer(("127.0.0.1", 0), MockProvider)
    threading.Thread(target=mock.serve_forever, daemon=True).start()
    (home / "config.toml").write_text(
        'model = "mock-1"\nmodel_provider = "mock"\napproval_policy = "never"\nsandbox_mode = "read-only"\n'
        f'[model_providers.mock]\nname = "mock"\nbase_url = "http://127.0.0.1:{mock.server_port}/v1"\n'
        'wire_api = "responses"\n')
    for name in [key for key in os.environ if key.startswith("CODEX_")]:
        del os.environ[name]
    os.environ["CODEX_HOME"] = str(home)
    results = {}
    host = Host(home)
    try:
        for scenario_function in SCENARIOS:
            scenario = Scenario(host, home, scenario_function.__name__)
            try:
                results[scenario.name] = scenario_function(scenario)
            except Exception as error:                     # noqa: BLE001 - reported, judged by the test
                results[scenario.name] = {"error": f"{type(error).__name__}: {error}"}
            finally:
                scenario.store.close()
    finally:
        host.stop()
        mock.shutdown()
    results["home"] = str(home)
    output.write_text(json.dumps(results, indent=1))


if __name__ == "__main__":
    main(Path(sys.argv[1]))
