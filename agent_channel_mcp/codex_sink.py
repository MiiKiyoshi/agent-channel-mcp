"""Deliver a message to a Codex thread through the app-server's own queue API, once.

`codex queue` hands the text over with a fresh random id every time it is run, so a
retry after a lost answer put the same message into the thread twice. The app-server
keeps two durable records a sender can read: the thread's queue (thread/queue/list)
and its items (thread/items/list), and a queued message carries the caller's
clientUserMessageId into the item it becomes. A delivery is therefore keyed
(agent-channel:<channel_id>:<message_id>) and, before it is added, the queue and the
items are read for that key: found with the same text, it was delivered; found with
other text, the key was reused and that is a conflict; found nowhere, it is added.
The app-server does not do this for us: it deletes a queued message when its turn
starts and records the item only later, so a read can find a delivered message in
neither place. The caller therefore never adds a message it handed over once; found
nowhere, the message is uncertain and is looked for again. The API is experimental
and may be absent: that is reported, not worked around.

The app-server is a stock `codex app-server --listen stdio://` child of the waiter.
It shares the queue database under CODEX_HOME with whatever process hosts the
thread (the Codex TUI, or the daemon), which is the same path `codex queue` takes.
"""

import json
import os
import select
import signal
import subprocess
import threading
import time

COMMAND = ["codex", "app-server", "--listen", "stdio://"]


class SinkError(Exception):
    """The app-server answered a request with an error."""


class SinkUnsupported(SinkError):
    """The queue API is not available on this app-server."""


class SinkTransportError(Exception):
    """No answer: the app-server ended, the read timed out, or the pipe broke."""


class KeyConflict(Exception):
    """The delivery key is already held by a message with other text."""


def _text_of(inputs: list) -> str:
    return "".join(part.get("text", "") for part in inputs if part.get("type") == "text")


class CodexSink:
    """One JSON-RPC connection to an app-server child, one JSON message per line."""

    def __init__(self, timeout: float = 60.0, stop: threading.Event | None = None,
                 command: list[str] = COMMAND):
        self.timeout = timeout
        self.stop = threading.Event() if stop is None else stop
        self._next_id = 0
        self._lock = threading.Lock()
        self._buffer = b""
        # A session of its own, so that ending it takes whatever it spawned too.
        self.process = subprocess.Popen(
            command, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL, start_new_session=True,
        )
        try:
            self.call("initialize", {
                "clientInfo": {"name": "agent-channel-mcp", "version": "0"},
                "capabilities": {"experimentalApi": True},
            })
            self._send({"jsonrpc": "2.0", "method": "initialized", "params": {}})
        except BaseException:
            self.close()
            raise

    def _send(self, message: dict) -> None:
        try:
            self.process.stdin.write(json.dumps(message).encode() + b"\n")
            self.process.stdin.flush()
        except (BrokenPipeError, OSError, ValueError) as error:
            raise SinkTransportError(f"app-server pipe closed: {error}") from error

    def _receive(self, deadline: float) -> dict:
        """The next JSON message from the child; reads wake every half second to look
        at `stop` and the deadline."""
        while True:
            newline = self._buffer.find(b"\n")
            if newline >= 0:
                line, self._buffer = self._buffer[:newline], self._buffer[newline + 1:]
                try:
                    return json.loads(line)
                except json.JSONDecodeError:
                    continue
            if self.stop.is_set():
                raise SinkTransportError("stopped before the app-server answered")
            if time.monotonic() >= deadline:
                raise SinkTransportError(f"app-server did not answer within {self.timeout:g} s")
            ready, _, _ = select.select([self.process.stdout], [], [], 0.5)
            if not ready:
                continue
            chunk = os.read(self.process.stdout.fileno(), 65536)
            if not chunk:
                raise SinkTransportError(f"app-server ended (exit {self.process.poll()})")
            self._buffer += chunk

    def call(self, method: str, params: dict | None = None) -> dict:
        """One request and its answer. No answer within the timeout, or `stop` set
        while waiting, ends the child: the caller starts a new sink next time."""
        with self._lock:
            self._next_id += 1
            request_id = self._next_id
            deadline = time.monotonic() + self.timeout
            try:
                self._send({"jsonrpc": "2.0", "id": request_id, "method": method, "params": params or {}})
                while True:
                    message = self._receive(deadline)
                    if message.get("id") == request_id and "method" not in message:
                        break
                    if "method" in message and "id" in message:
                        # A request from the server (an approval, say): not this client's.
                        self._send({"jsonrpc": "2.0", "id": message["id"],
                                    "error": {"code": -32601, "message": "not handled"}})
            except SinkTransportError:
                self.close()
                raise
        if "error" in message:
            text = str(message["error"].get("message", message["error"]))
            if "experimentalApi" in text or message["error"].get("code") == -32601:
                raise SinkUnsupported(text)
            raise SinkError(text)
        return message.get("result") or {}

    def close(self) -> None:
        if self.process.poll() is None:
            try:
                os.killpg(self.process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            try:
                self.process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                pass
        self.process.stdin.close()
        self.process.stdout.close()

    # --- the delivery itself ---

    def _pages(self, method: str, params: dict):
        """The entries of a paginated listing, one page at a time, so that a caller
        that stops at the first match reads no further page. A cursor seen before
        is a SinkError: the listing would never end."""
        cursor = None
        seen = set()
        while True:
            page = self.call(method, {**params, "cursor": cursor})
            yield from page.get("data", [])
            cursor = page.get("nextCursor")
            if not cursor:
                return
            if cursor in seen:
                raise SinkError(f"{method} repeats cursor {cursor!r}")
            seen.add(cursor)

    def find(self, thread_id: str, key: str, text: str) -> str | None:
        """Where a delivery with this key already is: "queued", "consumed", or None.
        The queue is read whole, then every item of the thread, newest first.
        A hit with other text is a KeyConflict."""
        for entry in self._pages("thread/queue/list", {"threadId": thread_id}):
            if entry.get("clientUserMessageId") == key:
                if _text_of(entry.get("input", [])) != text:
                    raise KeyConflict(f"key {key} is queued with other text")
                return "queued"
        for entry in self._pages("thread/items/list",
                                 {"threadId": thread_id, "sortDirection": "desc"}):
            item = entry.get("item", entry)
            if item.get("type") == "userMessage" and item.get("clientId") == key:
                if _text_of(item.get("content", [])) != text:
                    raise KeyConflict(f"key {key} was consumed with other text")
                return "consumed"
        return None

    def add(self, thread_id: str, key: str, text: str) -> str:
        """Queue the message under its key; the queued submission's id."""
        result = self.call("thread/queue/add", {
            "threadId": thread_id, "clientUserMessageId": key,
            "input": [{"type": "text", "text": text}],
        })
        return result["queuedSubmission"]["id"]
