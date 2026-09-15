"""At most one delivery per message id at a Codex thread: the key, the look instead
of a second add, the uncertain state, the conflict, the unsupported API, and the lock
between two waiters."""

import os
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from agent_channel_mcp import waiter as waiter_module
from agent_channel_mcp.codex_sink import CodexSink, KeyConflict, SinkError, SinkTransportError, SinkUnsupported
from agent_channel_mcp.store import Store
from agent_channel_mcp.waiter import DeliveryNotOwned, DeliveryUncertain, deliver_to_codex, render_message
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


# The pending pick with many excluded ids: one SQL variable, however many.

def test_the_pending_pick_skips_any_number_of_excluded_ids(tmp_path):
    store = Store(tmp_path / "db")
    plan = store.participant("room", "plan")
    execute = store.participant("room", "exec")
    store.activate(execute["id"], "tok")
    ids = [store.send(plan["id"], f"m{index}", to="exec")[0]["message_id"] for index in range(1201)]
    assert store.pending_for_token("tok", excluding=set(ids[:1200]))["id"] == ids[1200]
    assert store.pending_for_token("tok", excluding=set(ids)) is None
    assert store.pending_for_token("tok")["id"] == ids[0]
    store.close()


# The key and the look before the add.

def test_a_first_attempt_adds_without_reading_and_marks_the_message(tmp_path, monkeypatch):
    codex = FakeCodex(tmp_path / "bin").apply(monkeypatch)
    store, receiver, message_id = _store_with_message(tmp_path / "db")
    message = store.pending_for_token("tok")
    assert message["attempted"] == 0
    sink = CodexSink(timeout=5)
    try:
        assert deliver_to_codex(store, sink, "thread-1", message, render_message(message), "tok") == "added"
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
        deliver_to_codex(store, sink, "thread-1", message, text, "tok")
        message = store.pending_for_token("tok")                      # still unacknowledged, now marked
        assert deliver_to_codex(store, sink, "thread-1", message, text, "tok") == "consumed"
        codex.set(consume="never")
        other = store.send(store.participant("room", "plan")["id"], "second", to="exec")[0]["message_id"]
        store.ack_for_token("tok", message_id)
        second = store.pending_for_token("tok")
        assert second["id"] == other
        deliver_to_codex(store, sink, "thread-1", second, render_message(second), "tok")
        second = store.pending_for_token("tok")
        assert deliver_to_codex(store, sink, "thread-1", second, render_message(second), "tok") == "queued"
    finally:
        sink.close()
    assert codex.requests().count("thread/queue/add") == 2
    assert [text.split("\n")[1] for text in codex.texts("thread-1")] == ["hello", "second"]
    store.close()


# The gap: handed over once, in neither the queue nor the items.

def test_a_marked_message_found_nowhere_is_not_added_again_and_is_acknowledged_once_it_appears(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(waiter_module, "CODEX_QUEUE_TIMEOUT_SECONDS", 0.5)
    monkeypatch.setattr(waiter_module, "CODEX_RETRY_PAUSE_SECONDS", 0.2)
    monkeypatch.setattr(waiter_module, "CODEX_UNCERTAIN_PAUSE_SECONDS", 0.3)
    codex = FakeCodex(tmp_path / "bin", consume="in_transit", drop_add_response=True).apply(monkeypatch)
    store, receiver, message_id = _store_with_message(tmp_path / "db")
    stop = threading.Event()
    thread = _run_waiter(tmp_path / "db", stop)
    try:
        # The add is taken but never answered; the retry finds the message nowhere.
        run = wait_for(lambda: (
            (run := store.last_waiter(receiver["id"], "tok")) and run["last_error"]
            and "uncertain" in run["last_error"] and run
        ), timeout=12)
        assert run["last_error"] == f"delivery uncertain for message {message_id}; not re-adding"
        time.sleep(1.0)                                                      # several looks
        assert codex.requests().count("thread/queue/add") == 1
        assert codex.requests().count("thread/items/list") >= 2
        assert store.pending(receiver["id"])["id"] == message_id
        assert codex.transit("thread-1") and codex.items("thread-1") == []
        # A later message is served meanwhile, in the same state of doubt only if it
        # meets the same failure; here the add answers again.
        codex.set(drop_add_response=None, consume="immediate")
        later = store.send(store.participant("room", "plan")["id"], "later", to="exec")[0]["message_id"]
        wait_for(lambda: f"{later} room plan\nlater" in codex.texts("thread-1"), timeout=12)
        assert store.pending(receiver["id"])["id"] == message_id            # still first, still unacknowledged
        # The item becomes visible: the next look acknowledges, with no add.
        codex.arrive("thread-1")
        wait_for(lambda: store.pending(receiver["id"]) is None, timeout=12)
        assert codex.requests().count("thread/queue/add") == 2               # one per message
        assert sorted(codex.texts("thread-1")) == sorted([f"{later} room plan\nlater", f"{message_id} room plan\nhello"])
    finally:
        stop.set()
        thread.join(timeout=5)
        store.close()


def test_a_marked_message_that_never_appears_stays_pending_and_uncertain(tmp_path, monkeypatch):
    monkeypatch.setattr(waiter_module, "CODEX_QUEUE_TIMEOUT_SECONDS", 0.5)
    monkeypatch.setattr(waiter_module, "CODEX_RETRY_PAUSE_SECONDS", 0.2)
    monkeypatch.setattr(waiter_module, "CODEX_UNCERTAIN_PAUSE_SECONDS", 0.2)
    codex = FakeCodex(tmp_path / "bin", consume="in_transit", drop_add_response=True).apply(monkeypatch)
    store, receiver, message_id = _store_with_message(tmp_path / "db")
    stop = threading.Event()
    thread = _run_waiter(tmp_path / "db", stop)
    try:
        wait_for(lambda: (
            (run := store.last_waiter(receiver["id"], "tok")) and run["last_error"]
            and "uncertain" in run["last_error"]
        ))
        # From the first note on, the looks write nothing to the database.
        watcher = Store(tmp_path / "db")
        version = watcher.db.execute("PRAGMA data_version").fetchone()[0]
        looks = codex.requests().count("thread/items/list")
        time.sleep(2.0)
        assert codex.requests().count("thread/items/list") >= looks + 3
        assert watcher.db.execute("PRAGMA data_version").fetchone()[0] == version
        watcher.close()
        assert codex.requests().count("thread/queue/add") == 1
        assert store.pending(receiver["id"])["id"] == message_id
        assert store.last_waiter(receiver["id"], "tok")["last_error"] == (
            f"delivery uncertain for message {message_id}; not re-adding"
        )
    finally:
        stop.set()
        thread.join(timeout=5)
        store.close()


def test_two_uncertain_messages_and_a_later_acknowledgement_write_nothing_more(tmp_path, monkeypatch):
    """Both doubts are written once; the looks that follow, alternating between the
    two, and the acknowledgement of a later message in between, add no write."""
    monkeypatch.setattr(waiter_module, "CODEX_QUEUE_TIMEOUT_SECONDS", 0.5)
    monkeypatch.setattr(waiter_module, "CODEX_RETRY_PAUSE_SECONDS", 0.2)
    monkeypatch.setattr(waiter_module, "CODEX_UNCERTAIN_PAUSE_SECONDS", 0.2)
    codex = FakeCodex(tmp_path / "bin", consume="in_transit", drop_add_response=True).apply(monkeypatch)
    store, receiver, first = _store_with_message(tmp_path / "db", "one")
    plan = store.participant("room", "plan")
    second = store.send(plan["id"], "two", to="exec")[0]["message_id"]
    stop = threading.Event()
    thread = _run_waiter(tmp_path / "db", stop)
    try:
        wait_for(lambda: len(codex.transit("thread-1")) == 2, timeout=12)          # both handed over once
        wait_for(lambda: (
            (run := store.last_waiter(receiver["id"], "tok")) and run["last_error"]
            == f"delivery uncertain for message {second}; not re-adding"
        ), timeout=12)
        codex.set(drop_add_response=None, consume="immediate")
        later = store.send(plan["id"], "three", to="exec")[0]["message_id"]
        wait_for(lambda: store.pending(receiver["id"]) is not None
                 and f"{later} room plan\nthree" in codex.texts("thread-1")
                 and store.pending_for_token("tok", excluding={first, second}) is None)
        watcher = Store(tmp_path / "db")
        version = watcher.db.execute("PRAGMA data_version").fetchone()[0]
        looks = codex.requests().count("thread/items/list")
        wait_for(lambda: codex.requests().count("thread/items/list") >= looks + 6, timeout=12)   # >= 3 per message
        assert watcher.db.execute("PRAGMA data_version").fetchone()[0] == version
        watcher.close()
        assert codex.requests().count("thread/queue/add") == 3                       # each id once
        assert store.pending(receiver["id"])["id"] == first
        assert store.pending_for_token("tok", excluding={first})["id"] == second
    finally:
        stop.set()
        thread.join(timeout=5)
        store.close()


def test_the_delivery_raises_uncertain_for_a_marked_message_found_nowhere(tmp_path, monkeypatch):
    FakeCodex(tmp_path / "bin").apply(monkeypatch)
    store, receiver, message_id = _store_with_message(tmp_path / "db")
    store.mark_attempted(message_id, "tok")
    message = store.pending_for_token("tok")
    sink = CodexSink(timeout=5)
    try:
        with pytest.raises(DeliveryUncertain, match=f"message {message_id}; not re-adding"):
            deliver_to_codex(store, sink, "thread-1", message, render_message(message), "tok")
    finally:
        sink.close()
    store.close()


def test_an_answered_refusal_clears_the_mark_so_the_next_attempt_may_add(tmp_path, monkeypatch):
    codex = FakeCodex(tmp_path / "bin", add_error="thread is busy").apply(monkeypatch)
    store, receiver, message_id = _store_with_message(tmp_path / "db")
    message = store.pending_for_token("tok")
    sink = CodexSink(timeout=5)
    try:
        with pytest.raises(SinkError, match="thread is busy"):
            deliver_to_codex(store, sink, "thread-1", message, render_message(message), "tok")
        assert store.pending_for_token("tok")["attempted"] == 0
        codex.set(add_error=None)
        assert deliver_to_codex(store, sink, "thread-1", store.pending_for_token("tok"),
                                render_message(message), "tok") == "added"
    finally:
        sink.close()
    assert codex.requests().count("thread/queue/add") == 2
    store.close()


# The delivery lock held elsewhere: a stop ends the wait, a deadline bounds it.

def _hold_delivery_lock(db) -> subprocess.Popen:
    """Another process holding thread-1's delivery lock until it is killed."""
    script = (
        "import fcntl, sys, time\n"
        "handle = open(sys.argv[1], 'a+')\n"
        "fcntl.flock(handle, fcntl.LOCK_EX)\n"
        "print('held', flush=True)\n"
        "time.sleep(600)\n"
    )
    holder = subprocess.Popen([sys.executable, "-c", script, f"{db}.thread-1.deliver.lock"],
                              stdout=subprocess.PIPE, text=True)
    assert holder.stdout.readline().strip() == "held"
    return holder


def _marker() -> str:
    return f"sleep {30 + os.getpid() % 7}.{os.getpid() % 100:02d}"


def _still_running(marker: str) -> bool:
    return subprocess.run(["pgrep", "-f", f"^{marker}$"], capture_output=True, text=True).stdout.strip() != ""


def test_a_stop_while_waiting_for_the_delivery_lock_ends_the_waiter_and_its_app_server_at_once(
    tmp_path, monkeypatch
):
    marker = _marker()
    codex = FakeCodex(tmp_path / "bin", child=marker).apply(monkeypatch)
    store, receiver, message_id = _store_with_message(tmp_path / "db")
    holder = _hold_delivery_lock(tmp_path / "db")
    stop = threading.Event()
    thread = _run_waiter(tmp_path / "db", stop)
    try:
        wait_for(lambda: "initialize" in codex.requests() and _still_running(marker))
        time.sleep(0.5)                                              # now waiting for the lock
        assert codex.requests().count("thread/queue/add") == 0
        started = time.monotonic()
        stop.set()
        thread.join(timeout=1.0)
        assert not thread.is_alive() and time.monotonic() - started < 1.0
        wait_for(lambda: not _still_running(marker), timeout=3)      # the child app-server is gone
        assert store.pending_for_token("tok")["attempted"] == 0
        assert codex.requests().count("thread/queue/add") == 0
        assert store.last_waiter(receiver["id"], "tok")["detail"] == "stopped by the server"
    finally:
        holder.kill()
        holder.wait(timeout=5)
        holder.stdout.close()
        stop.set()
        thread.join(timeout=5)
    # The lock is free: a waiter delivers the message once.
    stop = threading.Event()
    thread = _run_waiter(tmp_path / "db", stop)
    try:
        wait_for(lambda: store.pending(receiver["id"]) is None)
        assert codex.texts("thread-1") == [f"{message_id} room plan\nhello"]
        assert codex.requests().count("thread/queue/add") == 1
    finally:
        stop.set()
        thread.join(timeout=5)
        store.close()


def test_a_delivery_lock_held_too_long_is_an_error_the_waiter_survives(tmp_path, monkeypatch):
    monkeypatch.setattr(waiter_module, "CODEX_LOCK_TIMEOUT_SECONDS", 0.5)
    monkeypatch.setattr(waiter_module, "CODEX_RETRY_PAUSE_SECONDS", 0.2)
    codex = FakeCodex(tmp_path / "bin").apply(monkeypatch)
    store, receiver, message_id = _store_with_message(tmp_path / "db")
    holder = _hold_delivery_lock(tmp_path / "db")
    stop = threading.Event()
    thread = _run_waiter(tmp_path / "db", stop)
    try:
        run = wait_for(lambda: (
            (run := store.last_waiter(receiver["id"], "tok")) and run["last_error"] and run
        ))
        assert run["last_error"] == (
            f"codex app-server delivery lock for thread thread-1 held elsewhere for 0.5 s for message {message_id}"
        )
        assert thread.is_alive() and run["ended_at"] is None
        assert store.pending_for_token("tok")["attempted"] == 0
        assert codex.requests().count("thread/queue/add") == 0
        holder.kill()
        holder.wait(timeout=5)
        wait_for(lambda: store.pending(receiver["id"]) is None)
        assert codex.requests().count("thread/queue/add") == 1
    finally:
        holder.stdout.close()
        if holder.poll() is None:
            holder.kill()
        stop.set()
        thread.join(timeout=5)
        store.close()


def test_the_lock_wait_stops_without_a_mark(tmp_path, monkeypatch):
    FakeCodex(tmp_path / "bin").apply(monkeypatch)
    store, receiver, message_id = _store_with_message(tmp_path / "db")
    holder = _hold_delivery_lock(tmp_path / "db")
    stop = threading.Event()
    sink = CodexSink(timeout=5, stop=stop)
    message = store.pending_for_token("tok")
    threading.Timer(0.3, stop.set).start()
    try:
        with pytest.raises(SinkTransportError, match="stopped before the delivery lock"):
            deliver_to_codex(store, sink, "thread-1", message, render_message(message), "tok")
    finally:
        sink.close()
        holder.kill()
        holder.wait(timeout=5)
        holder.stdout.close()
    assert store.pending_for_token("tok")["attempted"] == 0
    store.close()


# Pagination: the look stops at the first match, reads every page when there is none,
# and does not follow a cursor that repeats.

def test_the_look_reads_no_page_past_the_first_match(tmp_path, monkeypatch):
    codex = FakeCodex(tmp_path / "bin", page_size=2).apply(monkeypatch)
    sink = CodexSink(timeout=5)
    try:
        for index in range(5):
            sink.add("thread-1", f"k{index}", f"t{index}")
        before = codex.requests().count("thread/items/list")
        assert sink.find("thread-1", "k4", "t4") == "consumed"         # newest first: page one
        assert codex.requests().count("thread/items/list") == before + 1
        before = codex.requests().count("thread/items/list")
        assert sink.find("thread-1", "k0", "t0") == "consumed"         # the last page
        assert codex.requests().count("thread/items/list") == before + 3
        before = codex.requests().count("thread/items/list")
        assert sink.find("thread-1", "absent", "t") is None            # every page
        assert codex.requests().count("thread/items/list") == before + 3
    finally:
        sink.close()


def test_a_listing_whose_cursor_repeats_is_a_bounded_error(tmp_path, monkeypatch):
    codex = FakeCodex(tmp_path / "bin", page_size=1, cursor_cycle=True).apply(monkeypatch)
    sink = CodexSink(timeout=5)
    try:
        sink.add("thread-1", "k0", "t0")
        sink.add("thread-1", "k1", "t1")
        with pytest.raises(SinkError, match="repeats cursor"):
            sink.find("thread-1", "absent", "t")
        assert codex.requests().count("thread/queue/list") == 2         # the page, and the page again
        assert codex.requests().count("thread/items/list") == 0         # not reached
    finally:
        sink.close()


# A takeover between reading the message and handing it over.

def test_a_waiter_taken_over_after_reading_the_message_hands_nothing_over(tmp_path, monkeypatch):
    """The old waiter has the message in hand when the role moves to a new connection;
    its mark is refused, it adds nothing to its thread, and the new waiter, seeing no
    mark, delivers the message to the new thread."""
    codex = FakeCodex(tmp_path / "bin").apply(monkeypatch)
    store, receiver, message_id = _store_with_message(tmp_path / "db")
    taken_over = threading.Event()

    class TakenOverAtTheGate(Store):
        def attempted(self, message_id):
            if not taken_over.is_set():          # under the delivery lock, just before the mark
                Store(tmp_path / "db").activate(receiver["id"], "tok-2")
                taken_over.set()
            return super().attempted(message_id)

    monkeypatch.setattr(waiter_module, "Store", TakenOverAtTheGate)
    exit_code = waiter_module.run(tmp_path / "db", "tok", "thread-1", threading.Event())
    assert exit_code == 0 and taken_over.is_set()
    assert store.last_waiter(receiver["id"], "tok")["detail"] == "token inactive"
    assert codex.requests().count("thread/queue/add") == 0
    assert codex.texts("thread-1") == []
    assert store.pending(receiver["id"])["id"] == message_id
    assert store.pending_for_token("tok-2")["attempted"] == 0
    monkeypatch.setattr(waiter_module, "Store", Store)
    stop = threading.Event()
    thread = threading.Thread(target=waiter_module.run, args=(tmp_path / "db", "tok-2", "thread-2", stop), daemon=True)
    thread.start()
    try:
        wait_for(lambda: store.pending(receiver["id"]) is None)
        assert codex.texts("thread-2") == [f"{message_id} room plan\nhello"]
        assert codex.requests().count("thread/queue/add") == 1
    finally:
        stop.set()
        thread.join(timeout=5)
        store.close()


def test_the_delivery_refuses_a_message_whose_recipient_is_owned_elsewhere(tmp_path, monkeypatch):
    codex = FakeCodex(tmp_path / "bin").apply(monkeypatch)
    store, receiver, message_id = _store_with_message(tmp_path / "db")
    message = store.pending_for_token("tok")
    store.activate(receiver["id"], "tok-2")
    sink = CodexSink(timeout=5)
    try:
        with pytest.raises(DeliveryNotOwned):
            deliver_to_codex(store, sink, "thread-1", message, render_message(message), "tok")
    finally:
        sink.close()
    assert codex.requests().count("thread/queue/add") == 0
    assert store.pending_for_token("tok-2")["attempted"] == 0
    store.close()


# The conflict: the key is taken by other text.

def test_a_key_held_by_other_text_is_a_conflict_that_is_reported_once_and_never_added(tmp_path, monkeypatch):
    codex = FakeCodex(tmp_path / "bin").apply(monkeypatch)
    store, receiver, message_id = _store_with_message(tmp_path / "db")
    key = f"agent-channel:{store.channel_id}:{message_id}"
    sink = CodexSink(timeout=5)
    sink.add("thread-1", key, "something else")
    sink.close()
    store.mark_attempted(message_id, "tok")
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


# Two waiters at once, each having read the message unmarked: the lock makes the
# second read the mark the first wrote, and look instead of add.

def test_two_waiters_delivering_the_same_message_add_it_once(tmp_path, monkeypatch):
    codex = FakeCodex(tmp_path / "bin", hold_add_seconds=1).apply(monkeypatch)
    store, receiver, message_id = _store_with_message(tmp_path / "db")
    message = store.pending_for_token("tok")
    assert message["attempted"] == 0
    text = render_message(message)

    def deliver(_):
        own = Store(tmp_path / "db")
        sink = CodexSink(timeout=10)
        try:
            return deliver_to_codex(own, sink, "thread-1", message, text, "tok")
        finally:
            sink.close()
            own.close()

    with ThreadPoolExecutor(2) as pool:
        results = sorted(pool.map(deliver, range(2)))
    assert results == ["added", "consumed"]
    assert codex.requests().count("thread/queue/add") == 1
    assert codex.texts("thread-1") == [text]
    store.close()
