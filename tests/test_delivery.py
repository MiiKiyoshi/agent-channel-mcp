"""End-to-end delivery tests for the local agent channel."""

from __future__ import annotations

import os
from pathlib import Path
import selectors
import sqlite3
import subprocess
import sys
import time

import pytest

from agent_channel_mcp.store import PRESENCE_LEASE_SECONDS, Store
from agent_channel_mcp.server import Channel
from agent_channel_mcp.waiter import render_message


def wait_for(predicate, timeout: float = 4.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        value = predicate()
        if value:
            return value
        time.sleep(0.03)
    pytest.fail("timed out waiting for delivery state")


def make_store(tmp_path: Path) -> Store:
    return Store(tmp_path / "channel.sqlite3")


def send_one(store: Store, sender_id: str, recipient: str, text: str) -> int:
    return store.send(sender_id, text, to=recipient)[0]["message_id"]


def test_pending_replays_until_ack_and_ack_is_receiver_scoped(tmp_path):
    store = make_store(tmp_path)
    alice = store.participant("room", "alice")
    bob = store.participant("room", "bob")
    message_id = send_one(store, alice["id"], bob["name"], "hello")

    assert store.pending(bob["id"]) == {
        "id": message_id,
        "room": "room",
        "sender": alice["name"],
        "recipient": bob["name"],
        "text": "hello",
    }
    assert store.pending(bob["id"])["id"] == message_id
    store.ack(alice["id"], message_id)
    assert store.pending(bob["id"])["id"] == message_id
    store.ack(bob["id"], message_id)
    assert store.pending(bob["id"]) is None
    store.close()


def test_messages_are_persistent_ordered_and_isolated_by_room(tmp_path):
    db = tmp_path / "channel.sqlite3"
    first = Store(db)
    sender = first.participant("one", "sender")
    receiver = first.participant("one", "receiver")
    other_receiver = first.participant("two", "receiver")
    first_id = send_one(first, sender["id"], receiver["name"], "first")
    second_id = send_one(first, sender["id"], receiver["name"], "second")
    assert first.participant("one", "receiver") == receiver
    assert first.participants("two") == [other_receiver]
    first.close()

    reopened = Store(db)
    assert reopened.pending(receiver["id"])["id"] == first_id
    reopened.ack(receiver["id"], first_id)
    assert reopened.pending(receiver["id"])["id"] == second_id
    assert reopened.pending(other_receiver["id"]) is None
    reopened.close()


def test_send_rejects_unknown_recipient_and_cross_room_target(tmp_path):
    store = make_store(tmp_path)
    sender = store.participant("one", "sender")
    other = store.participant("two", "other")
    with pytest.raises(ValueError):
        store.send(sender["id"], "message", to="missing")
    with pytest.raises(ValueError):
        store.send(sender["id"], "message", to=other["name"])
    store.close()


def test_broadcast_targets_every_other_registered_participant(tmp_path):
    store = make_store(tmp_path)
    plan = store.participant("room", "plan")
    execute = store.participant("room", "exec")
    review = store.participant("room", "review")
    other = store.participant("other", "exec")

    deliveries = store.send(plan["id"], "all")
    assert [delivery["to"] for delivery in deliveries] == ["exec", "review"]
    assert store.pending(execute["id"])["text"] == "all"
    assert store.pending(review["id"])["text"] == "all"
    assert store.pending(plan["id"]) is None
    assert store.pending(other["id"]) is None
    store.close()


def test_rename_leave_and_rejoin_preserve_pending_messages(tmp_path):
    store = make_store(tmp_path)
    sender = store.participant("room", "plan")
    receiver = store.participant("room", "exec")
    message_id = send_one(store, sender["id"], receiver["name"], "pending")
    token = "session-token"
    store.activate(receiver["id"], token)

    renamed = store.rename(receiver["id"], "run")
    assert renamed == {**receiver, "name": "run"}
    assert [p["name"] for p in store.participants("room")] == ["plan", "run"]
    assert store.leave(receiver["id"], token) == renamed
    assert [p["name"] for p in store.participants("room")] == ["plan"]
    with pytest.raises(ValueError, match="Recipient"):
        store.send(sender["id"], "after leave", to="run")

    rejoined = store.participant("room", "run")
    assert rejoined == renamed
    assert store.pending(rejoined["id"])["id"] == message_id
    store.close()


def test_join_collects_inactive_rooms_without_pending_messages(tmp_path):
    store = make_store(tmp_path)
    store.participant("empty", "plan")

    used_sender = store.participant("used", "plan")
    used_receiver = store.participant("used", "exec")
    used_message = send_one(store, used_sender["id"], used_receiver["name"], "done")
    store.ack(used_receiver["id"], used_message)

    pending_sender = store.participant("pending", "plan")
    pending_receiver = store.participant("pending", "exec")
    send_one(store, pending_sender["id"], pending_receiver["name"], "keep")

    active = store.participant("active", "plan")
    store.activate(active["id"], "active-token")
    with store.db:
        store.db.execute(
            "UPDATE rooms SET last_activity=?", (int(time.time()) - 12 * 60 * 60 - 1,)
        )

    store.participant("trigger", "plan")
    rooms = {row["name"] for row in store.db.execute("SELECT name FROM rooms")}
    assert rooms == {"active", "pending", "trigger"}
    assert store.pending(pending_receiver["id"])["text"] == "keep"
    assert store.db.execute(
        "SELECT 1 FROM messages WHERE id=?", (used_message,)
    ).fetchone() is None
    store.close()


def test_existing_database_is_migrated_without_losing_participants(tmp_path):
    path = tmp_path / "channel.sqlite3"
    db = sqlite3.connect(path)
    db.executescript("""
        CREATE TABLE participants (
            id TEXT PRIMARY KEY, room TEXT NOT NULL, name TEXT NOT NULL,
            token TEXT, UNIQUE(room, name)
        );
        INSERT INTO participants(id, room, name, token)
            VALUES ('participant-id', 'room', 'plan', 'session-token');
        CREATE TABLE messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sender_id TEXT NOT NULL REFERENCES participants(id),
            recipient_id TEXT NOT NULL REFERENCES participants(id),
            text TEXT NOT NULL, acknowledged INTEGER NOT NULL DEFAULT 0
        );
        INSERT INTO messages(sender_id, recipient_id, text)
            VALUES ('participant-id', 'participant-id', 'kept');
        CREATE TABLE waiter_requests (
            participant_id TEXT PRIMARY KEY REFERENCES participants(id) ON DELETE CASCADE,
            token TEXT NOT NULL, codex_thread TEXT NOT NULL, requested_at INTEGER NOT NULL
        );
        INSERT INTO waiter_requests VALUES ('participant-id', 'session-token', 'thread-7', 5);
    """)
    db.close()

    store = Store(path)
    columns = {row["name"] for row in store.db.execute("PRAGMA table_info(participants)")}
    assert {"left_at", "connection_heartbeat_at"} <= columns
    assert store.participants("room") == [
        {"id": "participant-id", "room": "room", "name": "plan"}
    ]
    assert store.pending("participant-id")["text"] == "kept"
    assert store.db.execute("SELECT 1 FROM rooms WHERE name='room'").fetchone() is not None
    waiter_columns = {
        row["name"] for row in store.db.execute("PRAGMA table_info(waiter_runs)")
    }
    assert {"pid", "heartbeat_at", "exit_kind", "last_error"} <= waiter_columns
    request_columns = [
        row["name"] for row in store.db.execute("PRAGMA table_info(waiter_requests)")
    ]
    assert request_columns == ["token", "codex_thread", "requested_at", "request_id"]
    assert store.waiter_request("session-token") == {
        "codex_thread": "thread-7", "requested_at": 5, "request_id": None
    }
    store.close()


def test_stale_registered_roles_are_reported_offline(tmp_path):
    store = make_store(tmp_path)
    active = store.participant("room", "active")
    research = store.participant("room", "research")
    write = store.participant("room", "write")
    store.activate(active["id"], "active-token")
    store.activate(research["id"], "research-token")
    store.activate(write["id"], "write-token")
    active_run = store.waiter_started(active["id"], "active-token", 1001)
    write_run = store.waiter_started(write["id"], "write-token", 1002)
    now = int(time.time())
    stale = now - PRESENCE_LEASE_SECONDS - 1
    with store.db:
        store.db.execute(
            "UPDATE participants SET connection_heartbeat_at=? WHERE id IN (?, ?)",
            (stale, research["id"], write["id"]),
        )
        store.db.execute(
            "UPDATE waiter_runs SET heartbeat_at=? WHERE id=?", (stale, write_run)
        )

    assert store.registered_roles("room") == ["active", "research", "write"]
    assert store.role_statuses("room", now=now) == [
        {"name": "active", "connection": "active", "waiter": "active"},
        {"name": "research", "connection": "offline", "waiter": "offline"},
        {"name": "write", "connection": "offline", "waiter": "offline"},
    ]
    store.waiter_finished("active-token", "normal", 0, "test complete")
    assert store.last_waiter(active["id"], "active-token")["id"] == active_run
    store.close()


def _waiter_command(db: Path, token: str):
    return [
        sys.executable,
        "-m",
        "agent_channel_mcp.waiter",
        "--db",
        str(db),
        "--token",
        token,
    ]


def _active(store: Store, participant: dict, token: str):
    store.activate(participant["id"], token)


def _stop_waiter(process: subprocess.Popen, store: Store, token: str):
    store.deactivate(token)
    try:
        process.wait(timeout=2)
    except subprocess.TimeoutExpired:
        process.terminate()
        process.wait(timeout=2)
    if process.stdout:
        process.stdout.close()
    if process.stderr:
        process.stderr.close()


def _codex_stub(directory: Path, exit_code: int = 0) -> Path:
    executable = directory / "codex"
    executable.write_text(
        "#!/bin/sh\n"
        f"printf '%s\\0' \"$@\" >> \"$CODEX_ARGS_LOG\"\n"
        f"exit {exit_code}\n",
        encoding="utf-8",
    )
    executable.chmod(0o755)
    return executable


def test_waiter_delivers_text_and_acks_after_stdout_success(tmp_path):
    db = tmp_path / "channel.sqlite3"
    store = Store(db)
    sender = store.participant("room", "sender")
    receiver = store.participant("room", "receiver")
    token = "session-token"
    _active(store, receiver, token)
    process = subprocess.Popen(
        _waiter_command(db, token),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        message_id = send_one(store, sender["id"], receiver["name"], "hello")
        selector = selectors.DefaultSelector()
        selector.register(process.stdout, selectors.EVENT_READ)
        ready = selector.select(timeout=4)
        assert ready, process.stderr.read()
        header = process.stdout.readline()
        body = process.stdout.readline()
        selector.close()
        assert header == f"{message_id} room sender\n"
        assert body == "hello\n"
        wait_for(lambda: store.pending(receiver["id"]) is None)
    finally:
        _stop_waiter(process, store, token)
        store.close()


def test_waiter_preserves_pending_when_stdout_is_unwritable(tmp_path):
    db = tmp_path / "channel.sqlite3"
    store = Store(db)
    sender = store.participant("room", "sender")
    receiver = store.participant("room", "receiver")
    send_one(store, sender["id"], receiver["name"], "hello")
    token = "session-token"
    _active(store, receiver, token)
    with open(os.devnull, "w", encoding="utf-8") as stdin, open("/dev/full", "w") as full:
        process = subprocess.Popen(
            _waiter_command(db, token), stdin=stdin, stdout=full, stderr=subprocess.PIPE, text=True
        )
        wait_for(lambda: process.poll() is not None)
    assert store.pending(receiver["id"]) is not None
    store.close()


def test_waiter_retries_failed_codex_queue_without_ack(tmp_path):
    db = tmp_path / "channel.sqlite3"
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    log = tmp_path / "codex-args.log"
    _codex_stub(bin_dir, exit_code=7)
    store = Store(db)
    sender = store.participant("room", "sender")
    receiver = store.participant("room", "receiver")
    send_one(store, sender["id"], receiver["name"], "hello")
    token = "session-token"
    _active(store, receiver, token)
    environment = os.environ.copy()
    environment["PATH"] = f"{bin_dir}{os.pathsep}{environment['PATH']}"
    environment["CODEX_ARGS_LOG"] = str(log)
    process = subprocess.Popen(_waiter_command(db, token) + ["--codex", "thread-42"], env=environment, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    wait_for(lambda: log.exists() and len(log.read_text(encoding="utf-8").splitlines()) > 0)
    time.sleep(0.1)
    assert store.pending(receiver["id"]) is not None
    run = wait_for(lambda: store.last_waiter(receiver["id"], token))
    wait_for(lambda: store.last_waiter(receiver["id"], token)["last_error"])
    assert store.last_waiter(receiver["id"], token)["last_error"] == (
        f"codex queue exited 7 for message {store.pending(receiver['id'])['id']}"
    )
    assert run["ended_at"] is None
    _stop_waiter(process, store, token)
    store.close()


def test_waiter_codex_queue_acks_and_preserves_message_argument(tmp_path):
    db = tmp_path / "channel.sqlite3"
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    log = tmp_path / "codex-args.log"
    _codex_stub(bin_dir)
    store = Store(db)
    sender = store.participant("room", "sender")
    receiver = store.participant("room", "receiver")
    text = "$(touch " + str(tmp_path / "pwned") + ") ' \" café ☕"
    message_id = send_one(store, sender["id"], receiver["name"], text)
    token = "session-token"
    _active(store, receiver, token)
    environment = os.environ.copy()
    environment["PATH"] = f"{bin_dir}{os.pathsep}{environment['PATH']}"
    environment["CODEX_ARGS_LOG"] = str(log)
    process = subprocess.Popen(
        _waiter_command(db, token) + ["--codex", "thread-42"],
        env=environment, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    )
    try:
        wait_for(lambda: store.pending(receiver["id"]) is None)
        args = log.read_bytes().split(b"\0")[:-1]
        assert b"thread-42" in args
        delivered = args[args.index(b"--message") + 1].decode()
        assert delivered == f"{message_id} room sender\n{text}"
        assert not (tmp_path / "pwned").exists()
        assert message_id > 0
    finally:
        _stop_waiter(process, store, token)
        store.close()


@pytest.mark.parametrize("body,expected", [
    ("a" * 499, "a" * 499),
    ("a" * 500, "a" * 500),
    ("a" * 501, "a" * 500 + "\na"),
    ("a" * 1001, "a" * 500 + "\n" + "a" * 500 + "\na"),
    ("  a\t\n\n b \n", "  a\t\n\n b \n"),
    ("a" * 499 + "😀Z", "a" * 499 + "\n😀Z"),
    ("😀" * 251, "😀" * 250 + "\n😀"),
])
def test_forced_wrap_preserves_text_and_existing_newlines(body, expected):
    actual = render_message({"id": 42, "room": "room", "sender": "claude", "text": body})
    assert actual == "42 room claude\n" + expected
    assert all(len(line.encode("utf-16-le")) <= 1000 for line in actual.split("\n"))


def test_only_one_waiter_holds_participant_lock(tmp_path):
    db = tmp_path / "channel.sqlite3"
    store = Store(db)
    sender = store.participant("room", "sender")
    receiver = store.participant("room", "receiver")
    token = "session-token"
    _active(store, receiver, token)
    first = subprocess.Popen(_waiter_command(db, token), stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    selector = selectors.DefaultSelector()
    try:
        message_id = send_one(store, sender["id"], receiver["name"], "lock held")
        selector.register(first.stdout, selectors.EVENT_READ)
        assert selector.select(timeout=3)
        assert first.stdout.readline() == f"{message_id} room sender\n"
        assert first.stdout.readline() == "lock held\n"
        second = subprocess.run(_waiter_command(db, token), capture_output=True,
                                text=True, timeout=3)
        assert second.returncode != 0
        assert "Already running" in second.stderr
        assert first.poll() is None
    finally:
        selector.close()
        _stop_waiter(first, store, token)
        store.close()


def test_new_session_waiter_takes_over_after_token_change(tmp_path):
    db = tmp_path / "channel.sqlite3"
    store = Store(db)
    sender = store.participant("room", "sender")
    receiver = store.participant("room", "receiver")
    old_token = "old-session-token"
    new_token = "new-session-token"
    _active(store, receiver, old_token)
    old = subprocess.Popen(
        _waiter_command(db, old_token),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    new = None
    old_lock = db.with_name(f"{db.name}.{old_token}.wait.lock")
    new_lock = db.with_name(f"{db.name}.{new_token}.wait.lock")
    try:
        wait_for(lambda: old_lock.exists() and old_lock.read_text() == old_token)
        _active(store, receiver, new_token)
        new = subprocess.Popen(
            _waiter_command(db, new_token),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        wait_for(lambda: old.poll() is not None)
        wait_for(lambda: new_lock.exists() and new_lock.read_text() == new_token)
        message_id = send_one(store, sender["id"], receiver["name"], "after takeover")
        selector = selectors.DefaultSelector()
        selector.register(new.stdout, selectors.EVENT_READ)
        assert selector.select(timeout=3)
        assert new.stdout.readline() == f"{message_id} room sender\n"
        assert new.stdout.readline() == "after takeover\n"
        selector.close()
    finally:
        if old.poll() is None:
            old.terminate()
            old.wait(timeout=2)
        if new is not None:
            _stop_waiter(new, store, new_token)
        if old.stdout:
            old.stdout.close()
        if old.stderr:
            old.stderr.close()
        store.close()


def test_waiter_records_signal_exit_and_join_reports_abrupt_loss(tmp_path):
    channel = Channel(tmp_path / "channel.sqlite3")
    joined = channel.join("room", "plan")
    participant = joined["participant"]
    process = subprocess.Popen(
        _waiter_command(channel.store.path, channel.token),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        wait_for(lambda: channel.store.last_waiter(participant["id"], channel.token))
        process.terminate()
        assert process.wait(timeout=2) == 128 + 15
        run = channel.store.last_waiter(participant["id"], channel.token)
        assert run["exit_kind"] == "signal"
        assert run["exit_code"] == 128 + 15
        assert run["detail"] == "SIGTERM"
        signaled = channel.join("room", "plan")
        assert signaled["waiter"] == "offline"
        assert signaled["waiter_detail"]["reason"] == "SIGTERM"
        assert signaled["waiter_detail"]["exit_kind"] == "signal"

        replacement = subprocess.Popen(
            _waiter_command(channel.store.path, channel.token),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        wait_for(
            lambda: channel.store.last_waiter(participant["id"], channel.token)["id"]
            > run["id"]
        )
        replacement.kill()
        replacement.wait(timeout=2)
        replacement_run = channel.store.last_waiter(participant["id"], channel.token)
        stale_heartbeat = int(time.time()) - PRESENCE_LEASE_SECONDS - 1
        with channel.store.db:
            channel.store.db.execute(
                "UPDATE waiter_runs SET heartbeat_at=? WHERE id=?",
                (stale_heartbeat, replacement_run["id"]),
            )
        status = channel.join("room", "plan")
        assert status["waiter"] == "offline"
        assert status["waiter_detail"]["reason"] == "heartbeat lease expired"
        assert status["waiter_detail"]["last_seen_at"] == stale_heartbeat
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=2)
        if "replacement" in locals() and replacement.poll() is None:
            replacement.kill()
            replacement.wait(timeout=2)
        for candidate in (process, locals().get("replacement")):
            if candidate is not None and candidate.stdout:
                candidate.stdout.close()
            if candidate is not None and candidate.stderr:
                candidate.stderr.close()
        channel.close()


def test_mcp_managed_codex_waiter_outlives_launcher_and_delivers(
    tmp_path, monkeypatch
):
    db = tmp_path / "channel.sqlite3"
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    log = tmp_path / "codex-args.log"
    _codex_stub(bin_dir)
    monkeypatch.setenv("PATH", f"{bin_dir}{os.pathsep}{os.environ['PATH']}")
    monkeypatch.setenv("CODEX_ARGS_LOG", str(log))
    channel = Channel(db)
    sender = channel.store.participant("room", "sender")
    joined = channel.join("room", "receiver", "codex")
    receiver = joined["participant"]
    token = channel.token
    try:
        launcher = subprocess.run(
            _waiter_command(db, token)
            + ["--register", "--codex", "thread-42"],
            env=os.environ.copy(),
            capture_output=True,
            text=True,
            timeout=4,
        )
        assert launcher.returncode == 0, launcher.stderr
        run = wait_for(lambda: channel.store.last_waiter(receiver["id"], token))
        assert run["ended_at"] is None
        assert channel.join("room", "receiver", "codex")["waiter"] == "active"

        message_id = send_one(
            channel.store, sender["id"], receiver["name"], "managed"
        )
        wait_for(lambda: channel.store.pending(receiver["id"]) is None)
        args = log.read_bytes().split(b"\0")[:-1]
        assert args[args.index(b"--message") + 1] == (
            f"{message_id} room sender\nmanaged".encode()
        )
    finally:
        channel.store.deactivate(token)
        wait_for(
            lambda: (
                channel.store.last_waiter(receiver["id"], token) is not None
                and channel.store.last_waiter(receiver["id"], token)["ended_at"]
                is not None
            )
        )
        run = channel.store.last_waiter(receiver["id"], token)
        assert run["exit_kind"] == "normal"
        assert run["detail"] == "token inactive"
        channel.close()
