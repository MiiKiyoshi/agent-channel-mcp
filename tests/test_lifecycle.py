"""The threads and the waiter outlive a busy database, and only that: any other error
ends them with its cause on record, and a waiter that never started is reported."""

import fcntl
import os
import sqlite3
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest

from agent_channel_mcp import server as server_module, waiter as waiter_module
from agent_channel_mcp.server import Channel
from agent_channel_mcp.store import Store
from agent_channel_mcp.waiter import lock_path


def wait_for(predicate, timeout: float = 6.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(0.05)
    pytest.fail("timed out waiting")


def _hold_write_lock(db: Path, seconds: float) -> subprocess.Popen:
    """Another process that takes the write lock and keeps it for a while."""
    script = (
        "import sqlite3, sys, time\n"
        "c = sqlite3.connect(sys.argv[1], timeout=10)\n"
        "c.execute('BEGIN IMMEDIATE')\n"
        "print('held', flush=True)\n"
        f"time.sleep({seconds})\n"
        "c.commit()\n"
    )
    process = subprocess.Popen(
        [sys.executable, "-c", script, str(db)], stdout=subprocess.PIPE, text=True
    )
    assert process.stdout.readline().strip() == "held"
    return process


def _codex_stub(directory: Path, *, exit_code: int = 0, sleep: float = 0) -> Path:
    executable = directory / "codex"
    executable.write_text(
        "#!/bin/sh\n"
        f"sleep {sleep}\n"
        "printf '%s\\0' \"$@\" >> \"$CODEX_ARGS_LOG\"\n"
        f"exit {exit_code}\n",
        encoding="utf-8",
    )
    executable.chmod(0o755)
    return executable


def _heartbeat_of(db: Path, token: str) -> int:
    """When the connection was last seen: its presence file beside the database."""
    store = Store(db)
    try:
        return store.connection_seen_at(token) or 0
    finally:
        store.close()


def _waiter_command(db: Path, token: str) -> list[str]:
    return [sys.executable, "-m", "agent_channel_mcp.waiter", "--db", str(db), "--token", token]


# R1: the connection heartbeat thread.

def test_the_heartbeat_thread_waits_out_a_lock_held_longer_than_its_connection_waits(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(server_module, "HEARTBEAT_INTERVAL_SECONDS", 1)
    db = tmp_path / "db"
    channel = Channel(db)
    channel.join("room", "exec", "claude")
    participant = channel.token
    thread = channel.connection_thread
    holder = _hold_write_lock(db, 12)                 # longer than the 10 s a connection waits
    try:
        time.sleep(11.5)                              # the thread has met the busy error by now
        assert thread.is_alive()
        beats_while_held = _heartbeat_of(db, participant)
    finally:
        holder.wait(timeout=20)
    released = int(time.time())
    wait_for(lambda: _heartbeat_of(db, participant) >= released, timeout=8)
    assert thread.is_alive()
    assert channel.connection_failure is None
    assert "thread_failures" not in channel.join("room", "exec", "claude")
    assert _heartbeat_of(db, participant) >= beats_while_held
    channel.close()


def test_the_heartbeat_thread_ends_on_a_permanent_error_and_join_reports_it(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(server_module, "HEARTBEAT_INTERVAL_SECONDS", 1)
    db = tmp_path / "db"
    channel = Channel(db)
    channel.join("room", "exec", "claude")
    thread = channel.connection_thread
    time.sleep(1.5)                                   # the thread has opened its store and beaten once
    db.parent.chmod(0o000)                            # the presence file can no longer be touched
    try:
        wait_for(lambda: not thread.is_alive(), timeout=8)
    finally:
        db.parent.chmod(0o700)
    assert "PermissionError" in channel.connection_failure
    reported = channel.join("room", "exec", "claude")
    assert "PermissionError" in reported["thread_failures"]["connection"]
    channel.close()


# R2: the waiter supervisor thread.

def test_the_supervisor_waits_out_a_lock_and_then_starts_the_requested_waiter(
    tmp_path, monkeypatch
):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    _codex_stub(bin_dir)
    monkeypatch.setenv("PATH", f"{bin_dir}{os.pathsep}{os.environ['PATH']}")
    monkeypatch.setenv("CODEX_ARGS_LOG", str(tmp_path / "codex-args.log"))
    db = tmp_path / "db"
    channel = Channel(db)
    channel.join("room", "exec", "claude")
    token = channel.token
    holder = _hold_write_lock(db, 12)
    try:
        channel._start_supervisor(token)              # its Store() meets the busy error
        time.sleep(11.5)
        assert channel.supervisor.is_alive()
    finally:
        holder.wait(timeout=20)
    Store(db).request_waiter(token, "thread-1")
    run = wait_for(lambda: channel.store.last_waiter_for_token(token), timeout=8)
    assert run["ended_at"] is None
    wait_for(lambda: channel.store.waiter_request(token) is None)
    assert channel.supervisor.is_alive()
    assert channel.supervisor_failure is None
    channel.close()


# R3: a waiter that fails before it has a run row.

def _register(db: Path, token: str) -> subprocess.CompletedProcess:
    return subprocess.run(
        _waiter_command(db, token) + ["--register", "--codex", "thread-1"],
        env=os.environ.copy(), capture_output=True, text=True, timeout=20,
    )


def test_a_start_that_fails_for_a_while_is_tried_again_until_it_runs(tmp_path, monkeypatch):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    _codex_stub(bin_dir)
    monkeypatch.setenv("PATH", f"{bin_dir}{os.pathsep}{os.environ['PATH']}")
    monkeypatch.setenv("CODEX_ARGS_LOG", str(tmp_path / "codex-args.log"))
    db = tmp_path / "db"
    channel = Channel(db)
    channel.join("room", "exec", "codex")
    token = channel.token
    # Someone else holds this connection's lock file for a moment: the waiter cannot
    # take it, fails before its run row, and is started again once the file is free.
    held = lock_path(channel.store.path, token).open("a+")
    fcntl.flock(held, fcntl.LOCK_EX | fcntl.LOCK_NB)
    threading.Timer(2.0, lambda: fcntl.flock(held, fcntl.LOCK_UN)).start()
    registered = _register(db, token)
    assert registered.returncode == 0, registered.stderr
    run = channel.store.last_waiter_for_token(token)
    assert run["ended_at"] is None
    wait_for(lambda: channel.store.waiter_request(token) is None)
    assert channel.worker_failure is None
    channel.close()


def test_a_start_that_keeps_failing_is_reported_to_register_before_it_gives_up(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(server_module, "WAITER_START_WINDOW_SECONDS", 3)
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    _codex_stub(bin_dir)
    monkeypatch.setenv("PATH", f"{bin_dir}{os.pathsep}{os.environ['PATH']}")
    monkeypatch.setenv("CODEX_ARGS_LOG", str(tmp_path / "codex-args.log"))
    db = tmp_path / "db"
    channel = Channel(db)
    joined = channel.join("room", "exec", "codex")
    token = channel.token
    held = lock_path(channel.store.path, token).open("a+")
    fcntl.flock(held, fcntl.LOCK_EX | fcntl.LOCK_NB)
    started = time.monotonic()
    registered = _register(db, token)
    took = time.monotonic() - started
    assert registered.returncode != 0
    assert "Already running" in registered.stderr           # the cause, not a bare timeout
    assert "did not start" not in registered.stderr
    assert took < 12
    run = channel.store.last_waiter(joined["participant"]["id"], token)
    assert run["ended_at"] is not None and run["exit_kind"] == "error"
    assert "Already running" in run["detail"]
    assert channel.store.waiter_request(token) is None
    reported = channel.join("room", "exec", "codex")
    assert reported["waiter"] == "offline"
    assert "Already running" in reported["waiter_detail"]["reason"]
    fcntl.flock(held, fcntl.LOCK_UN)
    channel.close()


def test_a_request_while_this_connections_waiter_already_runs_is_simply_done(
    tmp_path, monkeypatch
):
    db = tmp_path / "db"
    channel = Channel(db)
    channel.join("room", "exec", "claude")
    token = channel.token
    process = subprocess.Popen(_waiter_command(db, token), stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE, text=True)
    try:
        run = wait_for(lambda: channel.store.last_waiter_for_token(token))
        assert run["ended_at"] is None
        rows_before = channel.store.db.execute("SELECT count(*) FROM waiter_runs").fetchone()[0]
        channel._start_supervisor(token)
        Store(db).request_waiter(token, "thread-1")
        wait_for(lambda: channel.store.waiter_request(token) is None)
        time.sleep(0.5)
        assert channel.store.db.execute("SELECT count(*) FROM waiter_runs").fetchone()[0] == rows_before
        assert channel.worker is None or not channel.worker.is_alive()
        assert channel.store.last_waiter_for_token(token)["ended_at"] is None
    finally:
        channel.store.deactivate(token)
        process.wait(timeout=5)
        channel.close()


# R4: the waiter itself.

def test_the_waiter_ends_on_a_permanent_error_with_its_cause_on_record(tmp_path):
    db = tmp_path / "db"
    channel = Channel(db)
    joined = channel.join("room", "exec", "claude")
    token = channel.token
    process = subprocess.Popen(_waiter_command(db, token), stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE, text=True)
    try:
        wait_for(lambda: channel.store.last_waiter_for_token(token))
        with sqlite3.connect(db, timeout=10) as connection:
            connection.execute("DROP TABLE messages")
        process.wait(timeout=5)
        assert process.returncode == 1
        run = channel.store.last_waiter(joined["participant"]["id"], token)
        assert run["ended_at"] is not None and run["exit_kind"] == "error"
        assert "no such table" in run["detail"]
        assert channel.join("room", "exec", "claude")["waiter"] == "offline"
    finally:
        if process.poll() is None:
            process.kill()
        channel.close()


def test_the_waiter_waits_out_a_busy_database_and_delivers_after(tmp_path):
    db = tmp_path / "db"
    channel = Channel(db)
    sender = channel.store.participant("room", "plan")
    joined = channel.join("room", "exec", "claude")
    token = channel.token
    message_id = channel.store.send(sender["id"], "through", to="exec")[0]["message_id"]
    holder = _hold_write_lock(db, 3)
    process = subprocess.Popen(_waiter_command(db, token), stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE, text=True)
    try:
        holder.wait(timeout=10)
        first = process.stdout.readline()
        assert first == f"{message_id} room plan\n"
        wait_for(lambda: channel.store.pending(joined["participant"]["id"]) is None)
        assert process.poll() is None
    finally:
        channel.store.deactivate(token)
        process.wait(timeout=5)
        channel.close()


def test_a_hung_codex_queue_is_cut_off_and_the_same_message_is_delivered_later(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(waiter_module, "CODEX_QUEUE_TIMEOUT_SECONDS", 0.5)
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    log = tmp_path / "codex-args.log"
    _codex_stub(bin_dir, sleep=5)
    monkeypatch.setenv("PATH", f"{bin_dir}{os.pathsep}{os.environ['PATH']}")
    monkeypatch.setenv("CODEX_ARGS_LOG", str(log))
    db = tmp_path / "db"
    store = Store(db)
    sender = store.participant("room", "plan")
    receiver = store.participant("room", "exec")
    token = "session-token"
    store.activate(receiver["id"], token)
    message_id = store.send(sender["id"], "slow lane", to="exec")[0]["message_id"]
    thread = threading.Thread(target=waiter_module.run, args=(db, token, "thread-1"), daemon=True)
    thread.start()
    try:
        run = wait_for(lambda: (
            (run := store.last_waiter(receiver["id"], token)) and run["last_error"] and run
        ), timeout=8)
        assert "timed out after 0.5 s" in run["last_error"]
        assert store.pending(receiver["id"])["id"] == message_id      # not acknowledged
        assert not log.exists()                                       # nothing got through
        _codex_stub(bin_dir)                                          # codex answers again
        wait_for(lambda: store.pending(receiver["id"]) is None, timeout=12)
        args = log.read_bytes().split(b"\0")[:-1]
        assert args[args.index(b"--message") + 1] == f"{message_id} room plan\nslow lane".encode()
    finally:
        store.deactivate(token)
        thread.join(timeout=5)
        store.close()


def test_a_message_whose_ack_met_a_busy_database_is_delivered_again_with_the_same_id(
    tmp_path, monkeypatch
):
    class AckBusyOnce(Store):
        busy_acks = 0

        def ack_for_token(self, token, message_id):
            if AckBusyOnce.busy_acks == 0:
                AckBusyOnce.busy_acks += 1
                raise sqlite3.OperationalError("database is locked")
            return super().ack_for_token(token, message_id)

    printed = []
    monkeypatch.setattr(waiter_module, "Store", AckBusyOnce)
    monkeypatch.setattr(waiter_module, "print", lambda *args, **kwargs: printed.append(args), raising=False)
    db = tmp_path / "db"
    store = Store(db)
    sender = store.participant("room", "plan")
    receiver = store.participant("room", "exec")
    token = "session-token"
    store.activate(receiver["id"], token)
    first = store.send(sender["id"], "once", to="exec")[0]["message_id"]
    second = store.send(sender["id"], "twice", to="exec")[0]["message_id"]
    thread = threading.Thread(target=waiter_module.run, args=(db, token), daemon=True)
    thread.start()
    try:
        wait_for(lambda: store.pending(receiver["id"]) is None, timeout=8)
    finally:
        store.deactivate(token)
        thread.join(timeout=5)
    delivered = [args[0].split("\n")[0] for args in printed if args and args[0][:1].isdigit()]
    assert delivered == [f"{first} room plan", f"{first} room plan", f"{second} room plan"]
    assert AckBusyOnce.busy_acks == 1
    store.close()


# A request and a run are told apart by ids, not by the second they were made in.

def test_finishing_a_request_leaves_a_newer_one_made_in_the_same_second(tmp_path, monkeypatch):
    monkeypatch.setattr(time, "time", lambda: 1800000000.0)
    store = Store(tmp_path / "db")
    participant = store.participant("room", "exec")
    store.activate(participant["id"], "tok")
    old = store.request_waiter("tok", "thread-old")
    read = store.waiter_request("tok")
    assert read["request_id"] == old
    new = store.request_waiter("tok", "thread-new")
    store.finish_waiter_request("tok", read["request_id"])          # the old one, already read
    assert store.waiter_request("tok") == {
        "codex_thread": "thread-new", "requested_at": 1800000000, "request_id": new,
    }
    store.cancel_waiter_request("tok", old)                          # register's own only
    assert store.waiter_request("tok")["request_id"] == new
    store.cancel_waiter_request("tok", new)
    assert store.waiter_request("tok") is None
    store.close()


def test_an_earlier_run_in_the_same_second_is_not_taken_for_this_attempt(tmp_path, monkeypatch):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    _codex_stub(bin_dir)
    monkeypatch.setenv("PATH", f"{bin_dir}{os.pathsep}{os.environ['PATH']}")
    monkeypatch.setenv("CODEX_ARGS_LOG", str(tmp_path / "codex-args.log"))
    db = tmp_path / "db"
    channel = Channel(db)
    joined = channel.join("room", "exec", "claude")
    token = channel.token
    store = channel.store
    store.waiter_started(joined["participant"]["id"], token, 4242)   # an earlier attempt's row,
    store.waiter_finished(token, "error", 1, "an earlier attempt")   # closed in this same second
    request_id = store.request_waiter(token, "thread-1")
    request = store.waiter_request(token)
    runs_before = store.latest_run_id(token)
    channel.worker_failure = {"request_id": request_id, "runs_before": runs_before,
                              "detail": "ValueError: Already running", "at": time.monotonic() - 1}
    channel._serve_request(store, token, request, threading.Event())
    assert store.waiter_request(token)["request_id"] == request_id  # not taken as recorded
    assert channel.worker is not None                                # tried again instead
    wait_for(lambda: store.latest_run_id(token) > runs_before)       # and this one ran
    channel.close()
