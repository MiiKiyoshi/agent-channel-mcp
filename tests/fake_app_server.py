"""A stand-in for `codex app-server --listen stdio://`, for the delivery tests.

`FakeCodex(directory)` puts a `codex` executable on PATH that runs this file. It
answers the four methods the sink uses and keeps its queue, items and request log in
a JSON file under `directory` so that the test, a second sink, or a restarted waiter
sees them. Its failures are knobs in that file, changeable while a sink is running:
  consume: "immediate" (default; a queued message becomes an item at once, as on an
           idle thread) or "never" (it stays queued)
  drop_add_response: thread/queue/add is applied but never answered
  hold_add_seconds: thread/queue/add is applied, then answered after this delay
  add_error: thread/queue/add answers this error message
  hang: no request is answered
  unsupported: the queue methods answer "requires experimentalApi capability"
  child: a shell command to leave running as a child, for cleanup tests
It is not the app-server: the move from queue to items is one step here, where the
real one has a gap between them.
"""

import fcntl
import json
import os
import subprocess
import sys
import time
import uuid
from pathlib import Path


def load(path: Path) -> dict:
    try:
        return json.loads(path.read_text())
    except FileNotFoundError:
        return {"threads": {}, "requests": [], "knobs": {}}


def save(path: Path, state: dict) -> None:
    with open(f"{path}.lock", "a+") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        Path(f"{path}.tmp").write_text(json.dumps(state))
        os.replace(f"{path}.tmp", path)


def thread_state(state: dict, thread_id: str) -> dict:
    return state["threads"].setdefault(thread_id, {"queue": [], "items": []})


def answer(message: dict, result=None, error=None) -> None:
    reply = {"jsonrpc": "2.0", "id": message["id"]}
    if error is not None:
        reply["error"] = error
    else:
        reply["result"] = result
    sys.stdout.write(json.dumps(reply) + "\n")
    sys.stdout.flush()


def main() -> None:
    path = Path(os.environ["FAKE_APP_SERVER_STATE"])
    child = load(path)["knobs"].get("child")
    if child:
        subprocess.Popen(["sh", "-c", child])
    for line in sys.stdin:
        try:
            message = json.loads(line)
        except json.JSONDecodeError:
            continue
        if "id" not in message:
            continue                                  # a notification
        method = message.get("method")
        params = message.get("params") or {}
        state = load(path)
        knobs = state["knobs"]
        state["requests"].append(method)
        save(path, state)
        if knobs.get("hang"):
            time.sleep(3600)
        if method == "initialize":
            answer(message, {"userAgent": "fake-app-server"})
            continue
        if knobs.get("unsupported") and method.startswith("thread/queue/"):
            answer(message, error={"code": -32600, "message": f"{method} requires experimentalApi capability"})
            continue
        thread = thread_state(state, params.get("threadId", ""))
        if method == "thread/queue/list":
            answer(message, {"data": list(thread["queue"]), "nextCursor": None})
        elif method == "thread/items/list":
            items = list(reversed(thread["items"])) if params.get("sortDirection") == "desc" else list(thread["items"])
            answer(message, {"data": [{"item": item} for item in items], "nextCursor": None})
        elif method == "thread/queue/add":
            if knobs.get("add_error"):
                answer(message, error={"code": -32600, "message": knobs["add_error"]})
                continue
            submission = {"id": str(uuid.uuid4()), "clientUserMessageId": params["clientUserMessageId"],
                          "input": params["input"]}
            if knobs.get("consume", "immediate") == "never":
                thread["queue"].append(submission)
            else:
                thread["items"].append({"type": "userMessage", "id": submission["id"],
                                        "clientId": submission["clientUserMessageId"],
                                        "content": submission["input"]})
            save(path, state)
            if knobs.get("drop_add_response"):
                time.sleep(3600)
            if knobs.get("hold_add_seconds"):
                time.sleep(knobs["hold_add_seconds"])
            answer(message, {"queuedSubmission": submission})
        else:
            answer(message, error={"code": -32601, "message": f"unknown method {method}"})


# --- the test-side handle ---

class FakeCodex:
    """The fake on PATH. `env` is the environment for a waiter process; `apply(monkeypatch)`
    sets the same for the test process (a waiter run in a thread)."""

    def __init__(self, directory: Path, **knobs):
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self.path = self.directory / "fake-state.json"
        executable = self.directory / "codex"
        executable.write_text(f'#!/bin/sh\nexec {sys.executable} {__file__}\n', encoding="utf-8")
        executable.chmod(0o755)
        self.env = dict(os.environ, PATH=f"{self.directory}{os.pathsep}{os.environ['PATH']}",
                        FAKE_APP_SERVER_STATE=str(self.path))
        if knobs:
            self.set(**knobs)

    def apply(self, monkeypatch) -> "FakeCodex":
        monkeypatch.setenv("PATH", self.env["PATH"])
        monkeypatch.setenv("FAKE_APP_SERVER_STATE", self.env["FAKE_APP_SERVER_STATE"])
        return self

    def set(self, **knobs) -> None:
        state = load(self.path)
        for name, value in knobs.items():
            if value is None:
                state["knobs"].pop(name, None)
            else:
                state["knobs"][name] = value
        save(self.path, state)

    def state(self) -> dict:
        return load(self.path)

    def requests(self) -> list[str]:
        return self.state()["requests"]

    def queue(self, thread_id: str) -> list[dict]:
        return self.state()["threads"].get(thread_id, {"queue": []})["queue"]

    def items(self, thread_id: str) -> list[dict]:
        return self.state()["threads"].get(thread_id, {"items": []})["items"]

    def texts(self, thread_id: str) -> list[str]:
        """What the thread received, consumed or still queued, in order of arrival."""
        state = self.state()["threads"].get(thread_id, {"queue": [], "items": []})
        entries = state["items"] + state["queue"]
        return ["".join(part["text"] for part in entry.get("content", entry.get("input", [])))
                for entry in entries]


if __name__ == "__main__":
    main()
