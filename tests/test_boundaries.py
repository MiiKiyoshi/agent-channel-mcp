"""Attacks on the boundaries: a takeover between a session's check and its write,
a constructor or a close that the database refuses, a queue that hangs with a
child, two registers at once, a commit that fails."""

import os
import sqlite3
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from agent_channel_mcp import waiter as waiter_module
from agent_channel_mcp.server import Channel
from agent_channel_mcp.store import Store


def wait_for(predicate, timeout: float = 6.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(0.05)
    pytest.fail("timed out waiting")


def _hold_write_lock(db: Path, seconds: float) -> subprocess.Popen:
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


def _messages(db: Path) -> int:
    with sqlite3.connect(db) as connection:
        return connection.execute("SELECT count(*) FROM messages").fetchone()[0]


# A takeover between the session's ownership check and its write.

def _taken_over_between(channel: Channel, method: str, thief: Store):
    """The store method runs only after another session has taken the identity."""
    participant = channel.identities["room"]["id"]
    original = getattr(channel.store, method)

    def wrapped(*args, **kwargs):
        thief.activate(participant, "thief-token")
        return original(*args, **kwargs)

    setattr(channel.store, method, wrapped)


def test_a_session_taken_over_after_its_check_sends_nothing(tmp_path):
    db = tmp_path / "db"
    channel = Channel(db)
    channel.join("room", "exec", "claude")
    channel.store.participant("room", "plan")
    _taken_over_between(channel, "send", Store(db))
    with pytest.raises(ValueError, match="no longer owns"):
        channel.send("hello", to="plan")
    assert _messages(db) == 0
    channel.close()


def test_a_session_taken_over_after_its_check_renames_nothing(tmp_path):
    db = tmp_path / "db"
    channel = Channel(db)
    channel.join("room", "exec", "claude")
    _taken_over_between(channel, "rename", Store(db))
    with pytest.raises(ValueError, match="no longer owns"):
        channel.rename("renamed")
    assert Store(db).registered_roles("room") == ["exec"]
    channel.close()


def test_a_session_taken_over_after_its_check_leaves_nothing(tmp_path):
    db = tmp_path / "db"
    channel = Channel(db)
    channel.join("room", "exec", "claude")
    _taken_over_between(channel, "leave", Store(db))
    with pytest.raises(ValueError, match="no longer owns"):
        channel.leave()
    assert Store(db).is_active(channel.identities["room"]["id"], "thief-token")
    channel.close()


def test_a_send_checks_ownership_again_on_every_attempt(tmp_path):
    """The first attempt is busy; the identity is taken over before the second."""
    db = tmp_path / "db"
    store = Store(db)
    plan = store.participant("room", "plan")
    store.participant("room", "exec")
    store.activate(plan["id"], "mine")
    thief = Store(db)
    attempts = []
    real = store._send_once

    def send_once(*args):
        attempts.append(1)
        if len(attempts) == 1:
            raise sqlite3.OperationalError("database is locked")
        thief.activate(plan["id"], "thief-token")
        return real(*args)

    store._send_once = send_once
    with pytest.raises(ValueError, match="no longer owns"):
        store.send(plan["id"], "hello", to="exec", token="mine")
    assert len(attempts) == 2
    assert _messages(db) == 0
    store.close()


def test_a_stale_waiter_start_after_a_takeover_opens_and_closes_nothing(tmp_path):
    db = tmp_path / "db"
    store = Store(db)
    participant = store.participant("room", "exec")
    store.activate(participant["id"], "old")
    rooms_read_by_old = store.token_participants("old")          # read, then delayed
    store.activate(participant["id"], "new")
    new_run = store.waiter_started(participant["id"], "new", 200)
    assert store.waiter_started(rooms_read_by_old[0], "old", 100) is None
    run = store.last_waiter(participant["id"])
    assert run["id"] == new_run and run["ended_at"] is None
    assert tuple(store.db.execute("SELECT token, count(*) FROM waiter_runs").fetchone()) == ("new", 1)
    store.close()


# A constructor or a close the database refuses.

def test_a_store_whose_setup_fails_closes_its_connection(tmp_path, monkeypatch):
    Store(tmp_path / "db").close()
    opened = []
    real_connect = sqlite3.connect

    class RefusesToBegin(sqlite3.Connection):
        def execute(self, sql, *args):
            if sql == "BEGIN IMMEDIATE":
                raise sqlite3.OperationalError("database is locked")
            return super().execute(sql, *args)

    def connect(*args, **kwargs):
        connection = real_connect(*args, factory=RefusesToBegin, **kwargs)
        opened.append(connection)
        return connection

    monkeypatch.setattr(sqlite3, "connect", connect)
    with pytest.raises(sqlite3.OperationalError, match="locked"):
        Store(tmp_path / "db")
    assert len(opened) == 1
    with pytest.raises(sqlite3.ProgrammingError, match="closed"):
        opened[0].execute("SELECT 1")


def test_a_close_the_database_refuses_still_stops_the_threads_and_closes(tmp_path):
    db = tmp_path / "db"
    channel = Channel(db)
    channel.join("room", "exec", "claude")
    script = channel.script
    holder = _hold_write_lock(db, 12)
    try:
        with pytest.raises(sqlite3.OperationalError, match="locked"):
            channel.close()
    finally:
        holder.wait(timeout=20)
    with pytest.raises(sqlite3.ProgrammingError, match="closed"):
        channel.store.db.execute("SELECT 1")
    assert not script.exists()
    time.sleep(0.5)
    assert not [t for t in threading.enumerate() if t.name.startswith("agent-channel")]


# A close over a waiter the server runs itself.

def _codex_that_sleeps(bin_dir: Path, marker: str) -> None:
    codex = bin_dir / "codex"
    codex.write_text(f"#!/bin/sh\n{marker} &\nwait\n", encoding="utf-8")
    codex.chmod(0o755)


def _marker() -> str:
    return f"sleep {20 + os.getpid() % 7}.{os.getpid() % 100:02d}"


def _still_running(marker: str) -> bool:
    return subprocess.run(["pgrep", "-f", f"^{marker}$"], capture_output=True, text=True).stdout.strip() != ""


def test_a_close_whose_sign_off_is_refused_still_stops_the_managed_waiter(tmp_path, monkeypatch):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    (bin_dir / "codex").write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    (bin_dir / "codex").chmod(0o755)
    monkeypatch.setenv("PATH", f"{bin_dir}{os.pathsep}{os.environ['PATH']}")
    db = tmp_path / "db"
    channel = Channel(db)
    joined = channel.join("room", "exec", "codex")
    token = channel.token
    Store(db).request_waiter(token, "thread-1")
    run = wait_for(lambda: channel.store.last_waiter_for_token(token))
    assert run["ended_at"] is None
    worker = wait_for(lambda: channel.worker)

    def refused(token):
        raise sqlite3.OperationalError("database is locked")

    channel.store.deactivate = refused
    with pytest.raises(sqlite3.OperationalError, match="locked"):
        channel.close()
    assert not worker.is_alive()                         # told to stop, not left to the sign-off
    assert channel.worker is None
    run = Store(db).last_waiter(joined["participant"]["id"], token)
    assert run["ended_at"] is not None and run["detail"] == "stopped by the server"
    assert Store(db).token_active(token)                 # the refused sign-off is not hidden


def test_a_close_during_a_queue_takes_the_queue_and_its_child_with_it(tmp_path, monkeypatch):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    marker = _marker()
    _codex_that_sleeps(bin_dir, marker)
    monkeypatch.setenv("PATH", f"{bin_dir}{os.pathsep}{os.environ['PATH']}")
    db = tmp_path / "db"
    channel = Channel(db)
    sender = channel.store.participant("room", "plan")
    joined = channel.join("room", "exec", "codex")
    token = channel.token
    channel.store.send(sender["id"], "hang", to="exec")
    Store(db).request_waiter(token, "thread-1")
    wait_for(lambda: _still_running(marker), timeout=8)
    worker = channel.worker
    channel.close()
    assert not worker.is_alive()
    wait_for(lambda: not _still_running(marker), timeout=3)
    checker = Store(db)
    assert checker.pending(joined["participant"]["id"]) is not None       # not acknowledged
    assert checker.last_waiter(joined["participant"]["id"], token)["detail"] == "stopped by the server"
    checker.close()


def test_a_waiter_signalled_during_a_queue_takes_the_queue_and_its_child_with_it(tmp_path, monkeypatch):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    marker = _marker()
    _codex_that_sleeps(bin_dir, marker)
    monkeypatch.setenv("PATH", f"{bin_dir}{os.pathsep}{os.environ['PATH']}")
    db = tmp_path / "db"
    store = Store(db)
    sender = store.participant("room", "plan")
    receiver = store.participant("room", "exec")
    store.activate(receiver["id"], "tok")
    store.send(sender["id"], "hang", to="exec")
    process = subprocess.Popen(
        [sys.executable, "-m", "agent_channel_mcp.waiter", "--db", str(db), "--token", "tok",
         "--codex", "thread-1"],
        env=os.environ.copy(), stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    )
    try:
        wait_for(lambda: _still_running(marker), timeout=8)
        process.terminate()
        assert process.wait(timeout=8) == 128 + 15
        wait_for(lambda: not _still_running(marker), timeout=3)
        run = store.last_waiter(receiver["id"], "tok")
        assert run["exit_kind"] == "signal" and run["detail"] == "SIGTERM"
        assert store.pending(receiver["id"]) is not None
    finally:
        if process.poll() is None:
            process.kill()
        store.close()


# What a restarted waiter queues again, and what it cannot help queueing again.

def _logging_codex(bin_dir: Path, after: str = "") -> None:
    codex = bin_dir / "codex"
    codex.write_text(
        "#!/bin/sh\n"
        "printf '%s\\0' \"$@\" >> \"$CODEX_ARGS_LOG\"\n"
        f"{after}\n"
        "exit 0\n",
        encoding="utf-8",
    )
    codex.chmod(0o755)


def _queued(log: Path) -> list[str]:
    if not log.exists():
        return []
    args = log.read_bytes().split(b"\0")[:-1]
    return [args[i + 1].decode().split("\n")[0] for i, arg in enumerate(args) if arg == b"--message"]


def _waiter(db: Path, token: str) -> subprocess.Popen:
    return subprocess.Popen(
        [sys.executable, "-m", "agent_channel_mcp.waiter", "--db", str(db), "--token", token,
         "--codex", "thread-1"],
        env=os.environ.copy(), stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True,
    )


def test_a_restarted_waiter_queues_only_what_was_never_acknowledged(tmp_path, monkeypatch):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    log = tmp_path / "sink.log"
    _logging_codex(bin_dir)
    monkeypatch.setenv("PATH", f"{bin_dir}{os.pathsep}{os.environ['PATH']}")
    monkeypatch.setenv("CODEX_ARGS_LOG", str(log))
    db = tmp_path / "db"
    store = Store(db)
    sender = store.participant("room", "plan")
    receiver = store.participant("room", "exec")
    store.activate(receiver["id"], "first-session")
    first = store.send(sender["id"], "one", to="exec")[0]["message_id"]
    second = store.send(sender["id"], "two", to="exec")[0]["message_id"]
    waiter = _waiter(db, "first-session")
    wait_for(lambda: store.pending(receiver["id"]) is None, timeout=10)
    store.deactivate("first-session")                       # the session ends; the waiter follows
    assert waiter.wait(timeout=10) == 0
    third = store.send(sender["id"], "three", to="exec")[0]["message_id"]
    store.activate(receiver["id"], "second-session")        # the same role, a new connection
    waiter = _waiter(db, "second-session")
    try:
        wait_for(lambda: store.pending(receiver["id"]) is None, timeout=10)
        time.sleep(1.0)                                     # long enough to queue again if it would
        assert _queued(log) == [f"{first} room plan", f"{second} room plan", f"{third} room plan"]
    finally:
        store.deactivate("second-session")
        waiter.wait(timeout=10)
        store.close()


def test_a_waiter_that_dies_after_the_queue_took_the_message_queues_it_again(tmp_path, monkeypatch):
    """The limit of at-least-once: accepted by the sink, not yet acknowledged, so the
    next waiter queues the same id once more. A sink that cannot take an id twice
    would need to say so itself."""
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    log = tmp_path / "sink.log"
    _logging_codex(bin_dir, after="kill -9 $PPID")           # the waiter dies right after acceptance
    monkeypatch.setenv("PATH", f"{bin_dir}{os.pathsep}{os.environ['PATH']}")
    monkeypatch.setenv("CODEX_ARGS_LOG", str(log))
    db = tmp_path / "db"
    store = Store(db)
    sender = store.participant("room", "plan")
    receiver = store.participant("room", "exec")
    store.activate(receiver["id"], "tok")
    only = store.send(sender["id"], "once, twice", to="exec")[0]["message_id"]
    waiter = _waiter(db, "tok")
    assert waiter.wait(timeout=10) == -9
    assert _queued(log) == [f"{only} room plan"]
    assert store.pending(receiver["id"])["id"] == only        # accepted, never acknowledged
    _logging_codex(bin_dir)                                   # the sink behaves from now on
    waiter = _waiter(db, "tok")
    try:
        wait_for(lambda: store.pending(receiver["id"]) is None, timeout=10)
        assert _queued(log) == [f"{only} room plan", f"{only} room plan"]
    finally:
        store.deactivate("tok")
        waiter.wait(timeout=10)
        store.close()


# A queue that hangs with a child of its own.

def test_a_timed_out_queue_takes_its_children_with_it(tmp_path, monkeypatch):
    monkeypatch.setattr(waiter_module, "CODEX_QUEUE_TIMEOUT_SECONDS", 0.5)
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    marker = f"sleep {20 + os.getpid() % 7}.{os.getpid() % 100:02d}"
    codex = bin_dir / "codex"
    codex.write_text(f"#!/bin/sh\n{marker} &\nwait\n", encoding="utf-8")
    codex.chmod(0o755)
    monkeypatch.setenv("PATH", f"{bin_dir}{os.pathsep}{os.environ['PATH']}")
    db = tmp_path / "db"
    store = Store(db)
    sender = store.participant("room", "plan")
    receiver = store.participant("room", "exec")
    store.activate(receiver["id"], "tok")
    store.send(sender["id"], "hang", to="exec")
    thread = threading.Thread(target=waiter_module.run, args=(db, "tok", "thread-1"), daemon=True)
    thread.start()
    try:
        run = wait_for(lambda: (
            (run := store.last_waiter(receiver["id"], "tok")) and run["last_error"] and run
        ), timeout=8)
        assert "timed out" in run["last_error"]
        time.sleep(0.3)
        left = subprocess.run(["pgrep", "-f", f"^{marker}$"], capture_output=True, text=True)
        assert left.stdout.strip() == "", left.stdout
        assert store.pending(receiver["id"]) is not None
    finally:
        store.deactivate("tok")
        thread.join(timeout=8)
        store.close()


# Two registers at once, and a commit that fails.

def test_two_registers_at_once_end_with_one_waiter_and_both_satisfied(tmp_path, monkeypatch):
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    (bin_dir / "codex").write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    (bin_dir / "codex").chmod(0o755)
    monkeypatch.setenv("PATH", f"{bin_dir}{os.pathsep}{os.environ['PATH']}")
    db = tmp_path / "db"
    channel = Channel(db)
    joined = channel.join("room", "exec", "codex")
    token = channel.token
    command = [sys.executable, "-m", "agent_channel_mcp.waiter", "--db", str(db), "--token", token,
               "--register", "--codex", "thread-1"]
    with ThreadPoolExecutor(2) as pool:
        results = list(pool.map(
            lambda _: subprocess.run(command, env=os.environ.copy(), capture_output=True,
                                     text=True, timeout=20), range(2)))
    assert [r.returncode for r in results] == [0, 0], [r.stderr for r in results]
    runs = channel.store.db.execute(
        "SELECT count(*) FROM waiter_runs WHERE participant_id=? AND ended_at IS NULL",
        (joined["participant"]["id"],),
    ).fetchone()[0]
    assert runs == 1
    wait_for(lambda: channel.store.waiter_request(token) is None)
    channel.close()


def test_a_commit_that_fails_leaves_no_message_and_a_usable_connection(tmp_path):
    db = tmp_path / "db"
    store = Store(db)
    plan = store.participant("room", "plan")
    execute = store.participant("room", "exec")
    real = store.db

    class CommitFailsOnce:
        failures = 0

        def __getattr__(self, name):
            return getattr(real, name)

        def __enter__(self):
            return real.__enter__()

        def __exit__(self, exc_type, exc, tb):
            if exc_type is None and CommitFailsOnce.failures == 0:
                CommitFailsOnce.failures += 1
                real.rollback()
                raise sqlite3.OperationalError("disk I/O error")
            return real.__exit__(exc_type, exc, tb)

    store.db = CommitFailsOnce()
    with pytest.raises(sqlite3.OperationalError, match="disk I/O error"):
        store.send(plan["id"], "lost at commit", to="exec")
    assert _messages(db) == 0
    store.db = real
    assert store.send(plan["id"], "after", to="exec")[0]["to"] == "exec"
    assert _messages(db) == 1
    store.close()
