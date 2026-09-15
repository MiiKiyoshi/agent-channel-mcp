"""Presence lives in files beside the database, and a store at the current schema
opens without the write lock: the heartbeats of many connections and waiters make no
durable database writes, and liveness is still told right after a crash, a takeover,
a second room, a close and at garbage collection."""

import asyncio
import contextlib
import json
import os
import shlex
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

import pytest
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from mcp.types import Implementation

from agent_channel_mcp.server import Channel
from agent_channel_mcp.store import PRESENCE_LEASE_SECONDS, SCHEMA_VERSION, Store


def wait_for(predicate, timeout: float = 8.0):
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


def _age(path: Path, at: int) -> None:
    os.utime(path, (at, at))


# Opening a store.

def test_a_store_at_the_current_schema_opens_while_the_write_lock_is_held(tmp_path):
    db = tmp_path / "db"
    Store(db).close()
    holder = _hold_write_lock(db, 12)
    try:
        started = time.monotonic()
        store = Store(db)
        assert time.monotonic() - started < 1.0
        assert store.db.execute("PRAGMA user_version").fetchone()[0] == SCHEMA_VERSION
        store.close()
    finally:
        holder.wait(timeout=20)


def test_an_older_store_is_migrated_once_with_its_data_kept(tmp_path):
    db = tmp_path / "db"
    store = Store(db)
    plan = store.participant("room", "plan")
    execute = store.participant("room", "exec")
    store.set_policy("room", "be brief")
    store.activate(execute["id"], "tok")
    message_id = store.send(plan["id"], "kept", to="exec")[0]["message_id"]
    store.db.execute("PRAGMA user_version = 0")          # as a database from before the version
    store.close()
    script = (
        "import sys, time\nfrom pathlib import Path\n"
        "from agent_channel_mcp.store import Store\n"
        "s = Store(Path(sys.argv[1])); print(s.db.execute('PRAGMA user_version').fetchone()[0]); s.close()\n"
    )
    env = dict(os.environ, PYTHONPATH=str(Path(__file__).resolve().parents[1]))
    openers = [subprocess.Popen([sys.executable, "-c", script, str(db)], env=env,
                                stdout=subprocess.PIPE, text=True) for _ in range(3)]
    assert [p.stdout.read().strip() for p in openers] == [str(SCHEMA_VERSION)] * 3
    assert [p.wait(timeout=20) for p in openers] == [0, 0, 0]
    store = Store(db)
    assert store.registered_roles("room") == ["exec", "plan"]
    assert store.policy("room") == "be brief"
    assert store.pending(execute["id"])["id"] == message_id
    assert store.token_active("tok")
    store.close()


def test_a_store_from_a_newer_code_is_refused(tmp_path):
    db = tmp_path / "db"
    store = Store(db)
    store.db.execute(f"PRAGMA user_version = {SCHEMA_VERSION + 1}")
    store.close()
    with pytest.raises(RuntimeError, match="schema version"):
        Store(db)


# Telling liveness from the files.

def test_presence_is_the_later_of_the_file_and_the_column_for_that_token_only(tmp_path):
    db = tmp_path / "db"
    store = Store(db)
    exec_ = store.participant("room", "exec")
    plan = store.participant("room", "plan")
    store.activate(exec_["id"], "tok-exec")
    store.activate(plan["id"], "tok-plan")
    now = int(time.time())
    stale = now - PRESENCE_LEASE_SECONDS - 1
    # A previous-version server writes only the column: seen through it.
    store._presence_path("connection", "tok-exec").unlink()
    assert store.role_statuses("room", now=now)[0]["connection"] == "active"
    # Column stale, file fresh (this version): seen through the file.
    with store.db:
        store.db.execute("UPDATE participants SET connection_heartbeat_at=? WHERE id=?", (stale, plan["id"]))
    assert store.role_statuses("room", now=now)[1]["connection"] == "active"
    # Both stale, or file absent and column stale: offline.
    _age(store._presence_path("connection", "tok-plan"), stale)
    with store.db:
        store.db.execute("UPDATE participants SET connection_heartbeat_at=? WHERE id=?", (stale, exec_["id"]))
    assert [s["connection"] for s in store.role_statuses("room", now=now)] == ["offline", "offline"]
    # A fresh file of another token vouches for nobody else.
    store._presence_touch("connection", "tok-someone-else")
    assert [s["connection"] for s in store.role_statuses("room", now=now)] == ["offline", "offline"]
    assert store.connection_seen_at("tok-plan") == stale
    store.close()


def test_a_token_that_cannot_name_a_file_is_refused(tmp_path):
    store = Store(tmp_path / "db")
    participant = store.participant("room", "exec")
    with pytest.raises(ValueError, match="token"):
        store.activate(participant["id"], "../escape")
    assert not store.token_active("../escape")
    store.close()


def test_a_crashed_waiter_is_offline_once_its_file_stops_moving(tmp_path):
    db = tmp_path / "db"
    channel = Channel(db)
    joined = channel.join("room", "exec", "claude")
    token = channel.token
    waiter = subprocess.Popen(
        [sys.executable, "-m", "agent_channel_mcp.waiter", "--db", str(db), "--token", token],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    try:
        wait_for(lambda: channel.join("room", "exec", "claude")["waiter"] == "active")
        waiter.kill()
        waiter.wait(timeout=5)
        seen = channel.store._presence_mtime("waiter", token)
        assert seen is not None
        later = seen + PRESENCE_LEASE_SECONDS + 1
        assert channel.store.role_statuses("room", now=later)[0]["waiter"] == "offline"
        run = channel.store.last_waiter(joined["participant"]["id"], token)
        assert run["ended_at"] is None and run["heartbeat_source"] == "file"
        assert run["heartbeat_at"] == seen
        # And the beat itself wrote nothing durable: the run row's column is its start.
        row = channel.store.db.execute("SELECT started_at, heartbeat_at FROM waiter_runs WHERE id=?", (run["id"],)).fetchone()
        assert row["heartbeat_at"] == row["started_at"]
        assert channel._waiter_detail(joined["participant"]["id"])["last_seen_source"] == "file"
    finally:
        if waiter.poll() is None:
            waiter.kill()
        channel.close()


def test_a_takeover_leaves_the_old_tokens_file_without_a_vote(tmp_path):
    db = tmp_path / "db"
    first = Channel(db)
    first.join("room", "exec", "claude")
    old_token = first.token
    second = Channel(db)
    second.join("room", "exec", "claude")               # takes the identity over
    new_token = second.token
    now = int(time.time())
    first.store._presence_touch("connection", old_token)      # the old session still beats
    _age(second.store._presence_path("connection", new_token), now - PRESENCE_LEASE_SECONDS - 1)
    with second.store.db:
        second.store.db.execute("UPDATE participants SET connection_heartbeat_at=NULL WHERE token=?", (new_token,))
    assert second.store.role_statuses("room", now=now)[0]["connection"] == "offline"
    second.store._presence_touch("connection", new_token)
    assert second.store.role_statuses("room", now=now)[0]["connection"] == "active"
    first.close()
    second.close()


def test_one_file_serves_every_room_of_a_connection(tmp_path):
    db = tmp_path / "db"
    channel = Channel(db)
    channel.join("alpha", "exec", "claude")
    channel.join("beta", "exec", "claude")
    token = channel.token
    now = int(time.time())
    with channel.store.db:
        channel.store.db.execute("UPDATE participants SET connection_heartbeat_at=NULL WHERE token=?", (token,))
    _age(channel.store._presence_path("connection", token), now - PRESENCE_LEASE_SECONDS - 1)
    assert channel.store.role_statuses("alpha", now=now)[0]["connection"] == "offline"
    assert channel.store.role_statuses("beta", now=now)[0]["connection"] == "offline"
    channel.store._presence_touch("connection", token)
    assert channel.store.role_statuses("alpha", now=now)[0]["connection"] == "active"
    assert channel.store.role_statuses("beta", now=now)[0]["connection"] == "active"
    assert len(list(db.parent.glob(f"db.{token}.conn"))) == 1
    channel.close()


def test_a_close_removes_the_file_and_the_role_reads_offline_at_once(tmp_path):
    db = tmp_path / "db"
    channel = Channel(db)
    channel.join("room", "exec", "claude")
    token = channel.token
    path = channel.store._presence_path("connection", token)
    assert path.exists()
    channel.close()
    assert not path.exists()
    store = Store(db)
    assert store.role_statuses("room")[0]["connection"] == "offline"
    assert store.connection_seen_at(token) is None
    store.close()


def test_garbage_collection_spares_a_room_whose_token_file_is_fresh(tmp_path, monkeypatch):
    import agent_channel_mcp.store as store_module
    monkeypatch.setattr(store_module, "ROOM_TTL_SECONDS", 0)
    db = tmp_path / "db"
    store = Store(db)
    participant = store.participant("old", "exec")
    store.activate(participant["id"], "tok")
    with store.db:
        store.db.execute("UPDATE rooms SET last_activity=0")
        store.db.execute("UPDATE participants SET connection_heartbeat_at=NULL")
    store._presence_touch("connection", "tok")                 # only the file says it lives
    store.participant("other", "someone")                       # join collects garbage
    assert store.registered_roles("old") == ["exec"]            # spared: its file is fresh
    _age(store._presence_path("connection", "tok"), int(time.time()) - PRESENCE_LEASE_SECONDS - 1)
    with store.db:
        store.db.execute("UPDATE rooms SET last_activity=0")
    store.participant("other2", "someone")
    assert store.registered_roles("old") == []
    store.close()


# Steady state with real servers and real waiters.

def test_six_servers_and_six_waiters_at_rest_make_no_durable_database_writes(tmp_path):
    async def scenario():
        db = tmp_path / "db"
        params = StdioServerParameters(
            command=str(Path(sys.executable).with_name("agent-channel-mcp")),
            args=["--db", str(db)],
            env=dict(os.environ),
        )
        waiters = []
        async with contextlib.AsyncExitStack() as stack:
            sessions = []
            for i in range(6):
                read, write = await stack.enter_async_context(stdio_client(params))
                session = await stack.enter_async_context(ClientSession(
                    read, write, client_info=Implementation(name="claude-code", version="test")))
                await session.initialize()
                sessions.append(session)
            tokens = []
            for i, session in enumerate(sessions):
                result = await session.call_tool("join", {"room": "rest", "name": f"r{i}"})
                joined = json.loads(result.content[0].text)
                command = shlex.split(joined["command"])          # "sh <script>"
                script_args = shlex.split(Path(command[-1]).read_text().splitlines()[1].removeprefix("exec "))
                token = script_args[script_args.index("--token") + 1]
                tokens.append(token)
                waiters.append(subprocess.Popen(
                    command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL))
            try:
                store = Store(db)
                def all_active():
                    statuses = store.role_statuses("rest")
                    return len(statuses) == 6 and all(
                        s["connection"] == "active" and s["waiter"] == "active" for s in statuses)
                deadline = time.monotonic() + 30
                while not all_active() and time.monotonic() < deadline:
                    await asyncio.sleep(0.5)
                assert all_active()
                probe = sqlite3.connect(db, timeout=10)
                wal = Path(str(db) + "-wal")
                probe.execute("SELECT count(*) FROM rooms").fetchone()
                before = (probe.execute("PRAGMA data_version").fetchone()[0], wal.stat().st_size)
                files_before = {t: store._presence_mtime("connection", t) for t in tokens}
                await asyncio.sleep(12)                           # two heartbeat intervals and more
                probe.execute("SELECT count(*) FROM rooms").fetchone()
                after = (probe.execute("PRAGMA data_version").fetchone()[0], wal.stat().st_size)
                assert after == before, f"a durable database write happened while at rest: {before} -> {after}"
                assert all_active()
                assert all(store._presence_mtime("connection", t) > files_before[t] for t in tokens)
                assert all(store._presence_mtime("waiter", t) is not None for t in tokens)
                probe.close()
                store.close()
            finally:
                for waiter in waiters:
                    waiter.terminate()
                for waiter in waiters:
                    with contextlib.suppress(Exception):
                        waiter.wait(timeout=5)
    asyncio.run(asyncio.wait_for(scenario(), timeout=120))
