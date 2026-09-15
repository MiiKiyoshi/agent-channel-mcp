"""One delivery per message id at a Codex thread: the key, the look before the add,
the conflict, the unsupported API, and the lock between two waiters."""

import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from agent_channel_mcp import waiter as waiter_module
from agent_channel_mcp.codex_sink import CodexSink, KeyConflict, SinkUnsupported
from agent_channel_mcp.store import Store
from agent_channel_mcp.waiter import deliver_to_codex, render_message
from fake_app_server import FakeCodex


def wait_for(predicate, timeout: float = 6.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(0.05)
    pytest.fail("timed out waiting")


def _store_with_message(db, text="hello"):
    store = Store(db)
    sender = store.participant("room", "plan")
    receiver = store.participant("room", "exec")
    store.activate(receiver["id"], "tok")
    message_id = store.send(sender["id"], text, to="exec")[0]["message_id"]
    return store, receiver, message_id


def _run_waiter(db, stop):
    thread = threading.Thread(target=waiter_module.run, args=(db, "tok", "thread-1", stop), daemon=True)
    thread.start()
    return thread


# The key and the look before the add.

def test_a_first_attempt_adds_without_reading_and_marks_the_message(tmp_path, monkeypatch):
    codex = FakeCodex(tmp_path / "bin").apply(monkeypatch)
    store, receiver, message_id = _store_with_message(tmp_path / "db")
    message = store.pending_for_token("tok")
    assert message["attempted"] == 0
    sink = CodexSink(timeout=5)
    try:
        assert deliver_to_codex(store, sink, "thread-1", message, render_message(message)) == "added"
    finally:
        sink.close()
    assert codex.requests() == ["initialize", "thread/queue/add"]
    assert codex.items("thread-1")[0]["clientId"] == f"agent-channel:{store.channel_id}:{message_id}"
    assert store.pending_for_token("tok")["attempted"] == 1
    store.close()


def test_a_repeated_attempt_finds_the_message_and_does_not_add_it_again(tmp_path, monkeypatch):
    codex = FakeCodex(tmp_path / "bin").apply(monkeypatch)
    store, receiver, message_id = _store_with_message(tmp_path / "db")
    message = store.pending_for_token("tok")
    text = render_message(message)
    sink = CodexSink(timeout=5)
    try:
        deliver_to_codex(store, sink, "thread-1", message, text)
        message = store.pending_for_token("tok")                      # still unacknowledged, now marked
        assert deliver_to_codex(store, sink, "thread-1", message, text) == "consumed"
        codex.set(consume="never")
        other = store.send(store.participant("room", "plan")["id"], "second", to="exec")[0]["message_id"]
        store.ack_for_token("tok", message_id)
        second = store.pending_for_token("tok")
        assert second["id"] == other
        deliver_to_codex(store, sink, "thread-1", second, render_message(second))
        second = store.pending_for_token("tok")
        assert deliver_to_codex(store, sink, "thread-1", second, render_message(second)) == "queued"
    finally:
        sink.close()
    assert codex.requests().count("thread/queue/add") == 2
    assert [text.split("\n")[1] for text in codex.texts("thread-1")] == ["hello", "second"]
    store.close()


# The conflict: the key is taken by other text.

def test_a_key_held_by_other_text_is_a_conflict_that_is_reported_once_and_never_added(tmp_path, monkeypatch):
    codex = FakeCodex(tmp_path / "bin").apply(monkeypatch)
    store, receiver, message_id = _store_with_message(tmp_path / "db")
    key = f"agent-channel:{store.channel_id}:{message_id}"
    sink = CodexSink(timeout=5)
    sink.add("thread-1", key, "something else")
    sink.close()
    store.mark_attempted(message_id)
    stop = threading.Event()
    thread = _run_waiter(tmp_path / "db", stop)
    try:
        run = wait_for(lambda: (
            (run := store.last_waiter(receiver["id"], "tok")) and run["last_error"] and run
        ))
        assert run["last_error"] == f"key conflict for message {message_id}: key {key} was consumed with other text"
        # A later message is still delivered; the conflicting one is left alone.
        later = store.send(store.participant("room", "plan")["id"], "after", to="exec")[0]["message_id"]
        wait_for(lambda: store.pending(receiver["id"]) is not None and store.pending(receiver["id"])["id"] == message_id
                 and len(codex.texts("thread-1")) == 2)
        assert codex.texts("thread-1") == ["something else", f"{later} room plan\nafter"]
        time.sleep(0.5)
        assert codex.requests().count("thread/queue/add") == 2                # never added
        assert store.pending(receiver["id"])["id"] == message_id            # never acknowledged
    finally:
        stop.set()
        thread.join(timeout=5)
        store.close()


# The unsupported API: nothing is sent, and why is on record.

def test_an_app_server_without_the_queue_api_delivers_nothing_and_says_so(tmp_path, monkeypatch):
    monkeypatch.setattr(waiter_module, "CODEX_UNSUPPORTED_PAUSE_SECONDS", 0.5)
    codex = FakeCodex(tmp_path / "bin", unsupported=True).apply(monkeypatch)
    store, receiver, message_id = _store_with_message(tmp_path / "db")
    stop = threading.Event()
    thread = _run_waiter(tmp_path / "db", stop)
    try:
        run = wait_for(lambda: (
            (run := store.last_waiter(receiver["id"], "tok")) and run["last_error"] and run
        ))
        assert run["last_error"] == (
            "codex app-server queue API unsupported: thread/queue/add requires experimentalApi capability"
        )
        assert codex.texts("thread-1") == []
        assert store.pending(receiver["id"])["id"] == message_id
        codex.set(unsupported=None)                                          # a capable app-server appears
        wait_for(lambda: store.pending(receiver["id"]) is None)
        assert codex.texts("thread-1") == [f"{message_id} room plan\nhello"]
    finally:
        stop.set()
        thread.join(timeout=5)
        store.close()


def test_the_sink_raises_unsupported_on_the_capability_error(tmp_path, monkeypatch):
    FakeCodex(tmp_path / "bin", unsupported=True).apply(monkeypatch)
    sink = CodexSink(timeout=5)
    try:
        with pytest.raises(SinkUnsupported, match="experimentalApi"):
            sink.find("thread-1", "k", "t")
    finally:
        sink.close()


# Two waiters at once: the lock makes one look after the other's add.

def test_two_waiters_delivering_the_same_marked_message_add_it_once(tmp_path, monkeypatch):
    codex = FakeCodex(tmp_path / "bin", hold_add_seconds=1).apply(monkeypatch)
    store, receiver, message_id = _store_with_message(tmp_path / "db")
    store.mark_attempted(message_id)
    message = store.pending_for_token("tok")
    text = render_message(message)

    def deliver(_):
        own = Store(tmp_path / "db")
        sink = CodexSink(timeout=10)
        try:
            return deliver_to_codex(own, sink, "thread-1", message, text)
        finally:
            sink.close()
            own.close()

    with ThreadPoolExecutor(2) as pool:
        results = sorted(pool.map(deliver, range(2)))
    assert results == ["added", "consumed"]
    assert codex.requests().count("thread/queue/add") == 1
    assert codex.texts("thread-1") == [text]
    store.close()
