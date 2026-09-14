"""send waits out another writer for a bounded time, retries the whole write, and
raises everything else at once."""

import sqlite3
import subprocess
import sys
import time
from pathlib import Path

import pytest

from agent_channel_mcp.store import Store, _is_busy


def _room(db: Path):
    store = Store(db)
    plan = store.participant("room", "plan")
    execute = store.participant("room", "exec")
    review = store.participant("room", "review")
    return store, plan, execute, review


def _count(db: Path, recipient_id: str) -> int:
    with sqlite3.connect(db) as connection:
        return connection.execute(
            "SELECT count(*) FROM messages WHERE recipient_id=?", (recipient_id,)
        ).fetchone()[0]


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


def _busy_timeout(store: Store) -> int:
    return store.db.execute("PRAGMA busy_timeout").fetchone()[0]


class _FailingConnection:
    """The store's connection, with execute raising once at a chosen statement."""

    def __init__(self, connection, fail_on: str, error: sqlite3.OperationalError):
        self._connection = connection
        self._fail_on = fail_on
        self._error = error
        self.failures = 0

    def execute(self, sql, *args):
        cursor = self._connection.execute(sql, *args)
        if sql.startswith(self._fail_on) and not self.failures:
            self.failures += 1
            raise self._error
        return cursor

    def __getattr__(self, name):
        return getattr(self._connection, name)

    def __enter__(self):
        return self._connection.__enter__()

    def __exit__(self, *exc):
        return self._connection.__exit__(*exc)


@pytest.mark.parametrize("to", ["exec", None])
def test_a_writer_holding_the_lock_longer_than_one_attempt_is_waited_out(tmp_path, to):
    db = tmp_path / "db"
    store, plan, execute, review = _room(db)
    hold = Store.SEND_BUSY_TIMEOUT_SECONDS + 0.5      # one attempt cannot outlast it
    holder = _hold_write_lock(db, hold)
    try:
        started = time.monotonic()
        deliveries = store.send(plan["id"], "through the lock", to=to)
        took = time.monotonic() - started
    finally:
        holder.wait(timeout=10)
    assert hold - 0.5 <= took < Store.SEND_DEADLINE_SECONDS
    assert [d["to"] for d in deliveries] == (["exec"] if to else ["exec", "review"])
    assert _count(db, execute["id"]) == 1                # once, not once per attempt
    assert _count(db, review["id"]) == (0 if to else 1)
    assert _count(db, plan["id"]) == 0
    assert _busy_timeout(store) == 10000                # other operations keep theirs
    store.close()


def _commit_in_a_loop(db: Path, seconds: float, hold: float) -> subprocess.Popen:
    """Another process committing as a heartbeat does, each commit keeping the write
    lock for `hold` seconds, as a commit does while its fsync waits on a loaded disk."""
    script = (
        "import sqlite3, sys, time\n"
        "c = sqlite3.connect(sys.argv[1], timeout=10)\n"
        "print('running', flush=True)\n"
        f"end = time.monotonic() + {seconds}\n"
        "while time.monotonic() < end:\n"
        "    c.execute('BEGIN IMMEDIATE')\n"
        "    c.execute(\"UPDATE rooms SET last_activity=last_activity WHERE name='room'\")\n"
        f"    time.sleep({hold})\n"
        "    c.commit()\n"
    )
    process = subprocess.Popen(
        [sys.executable, "-c", script, str(db)], stdout=subprocess.PIPE, text=True
    )
    assert process.stdout.readline().strip() == "running"
    return process


def test_a_store_connection_fsyncs_at_checkpoints_rather_than_under_the_write_lock(tmp_path):
    store = Store(tmp_path / "db")
    assert store.db.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
    assert store.db.execute("PRAGMA synchronous").fetchone()[0] == 1      # NORMAL
    store.close()


def test_sends_and_reads_go_through_while_heartbeats_keep_the_lock_nearly_always(tmp_path):
    """Three other processes commit back to back, each commit holding the lock 150 ms:
    the lock is taken most of the time, as it was with a heartbeat per server and per
    waiter stalled on fsync. Every send still lands once, and reads never wait."""
    db = tmp_path / "db"
    store, plan, execute, review = _room(db)
    writers = [_commit_in_a_loop(db, 8, 0.15) for _ in range(3)]
    try:
        started = time.monotonic()
        for round_ in range(4):
            to = "exec" if round_ % 2 == 0 else None
            deliveries = store.send(plan["id"], f"round {round_}", to=to)
            assert [d["to"] for d in deliveries] == (["exec"] if to else ["exec", "review"])
            assert store.is_active(plan["id"], "none") is False      # a read, at once
            assert store.pending_for_token("none") is None
        took = time.monotonic() - started
    finally:
        for writer in writers:
            writer.wait(timeout=20)
    assert took < 4 * Store.SEND_DEADLINE_SECONDS
    assert _count(db, execute["id"]) == 4
    assert _count(db, review["id"]) == 2
    assert _count(db, plan["id"]) == 0
    store.close()


def test_a_writer_holding_the_lock_past_the_deadline_fails_within_it_and_writes_nothing(tmp_path):
    db = tmp_path / "db"
    store, plan, execute, review = _room(db)
    holder = _hold_write_lock(db, Store.SEND_DEADLINE_SECONDS + 1)
    try:
        started = time.monotonic()
        with pytest.raises(sqlite3.OperationalError, match="^database is locked$"):
            store.send(plan["id"], "too long", to="exec")
        took = time.monotonic() - started
    finally:
        holder.wait(timeout=20)
    assert took <= Store.SEND_DEADLINE_SECONDS
    assert _count(db, execute["id"]) == 0
    assert _busy_timeout(store) == 10000
    assert store.send(plan["id"], "afterwards", to="exec")[0]["to"] == "exec"   # connection clean
    assert _count(db, execute["id"]) == 1
    store.close()


def test_a_busy_attempt_is_rolled_back_whole_before_the_next_one(tmp_path):
    """The busy error arrives after the first recipient's row was written."""
    db = tmp_path / "db"
    store, plan, execute, review = _room(db)
    store.db = _FailingConnection(
        store.db, "INSERT INTO messages", sqlite3.OperationalError("database is locked")
    )
    deliveries = store.send(plan["id"], "broadcast", to=None)
    assert store.db.failures == 1
    assert [d["to"] for d in deliveries] == ["exec", "review"]
    assert _count(db, execute["id"]) == 1
    assert _count(db, review["id"]) == 1
    store.close()


def test_an_error_that_is_not_busy_is_raised_at_once(tmp_path):
    db = tmp_path / "db"
    store, plan, execute, review = _room(db)
    with sqlite3.connect(db) as connection:
        connection.execute("DROP TABLE messages")
    started = time.monotonic()
    with pytest.raises(sqlite3.OperationalError, match="no such table"):
        store.send(plan["id"], "nowhere to go", to="exec")
    assert time.monotonic() - started < 0.5
    assert _busy_timeout(store) == 10000
    store.close()


def test_busy_is_told_by_code_where_python_carries_one_and_by_message_otherwise(tmp_path):
    db = tmp_path / "db"
    store = Store(db)
    holder = _hold_write_lock(db, 1.0)
    try:
        store.db.execute("PRAGMA busy_timeout = 10")
        with pytest.raises(sqlite3.OperationalError) as raised:
            store.db.execute("BEGIN IMMEDIATE")
    finally:
        holder.wait(timeout=10)
    assert _is_busy(raised.value)
    if sys.version_info >= (3, 11):
        assert raised.value.sqlite_errorcode == 5
    # Python 3.10 has no sqlite_errorcode; a constructed error carries None on 3.11+.
    constructed = sqlite3.OperationalError("database is locked")
    assert getattr(constructed, "sqlite_errorcode", None) is None
    assert _is_busy(constructed)
    assert not _is_busy(sqlite3.OperationalError("database table is locked"))   # SQLITE_LOCKED
    assert not _is_busy(sqlite3.OperationalError("no such table: messages"))
    store.close()
