"""One connection in several rooms, served by one waiter."""

import selectors
import sqlite3
import subprocess
import sys
from pathlib import Path

import pytest

from agent_channel_mcp.server import Channel
from agent_channel_mcp.store import Store
from test_delivery import wait_for


def _waiter(db: Path, token: str) -> subprocess.Popen:
    return subprocess.Popen(
        [sys.executable, "-m", "agent_channel_mcp.waiter", "--db", str(db), "--token", token],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    )


def _read_delivery(process: subprocess.Popen) -> tuple[str, str]:
    selector = selectors.DefaultSelector()
    selector.register(process.stdout, selectors.EVENT_READ)
    try:
        assert selector.select(timeout=4), process.stderr.readline()
    finally:
        selector.close()
    return process.stdout.readline().rstrip("\n"), process.stdout.readline().rstrip("\n")


def _finish(process: subprocess.Popen) -> None:
    if process.poll() is None:
        process.terminate()
        process.wait(timeout=2)
    process.stdout.close()
    process.stderr.close()


def test_second_room_joins_the_running_waiter(tmp_path):
    db = tmp_path / "db"
    agent = Channel(db)
    peer = Channel(db)
    process = None
    try:
        first = agent.join("r1", "exec", "claude-code")
        assert first["waiter"] == "offline" and first["rooms"] == ["r1"]
        process = _waiter(db, agent.token)
        wait_for(lambda: agent.store.last_waiter_for_token(agent.token))

        second = agent.join("r2", "exec", "claude-code")
        assert second["waiter"] == "active"
        assert second["rooms"] == ["r1", "r2"]
        assert "command" not in second and "how" not in second

        peer.join("r2", "plan")
        message = peer.send("second room", to="exec")["deliveries"][0]["message_id"]
        assert _read_delivery(process) == (f"{message} r2 plan", "second room")
        peer.join("r1", "plan")
        message = peer.send("first room", to="exec", room="r1")["deliveries"][0]["message_id"]
        assert _read_delivery(process) == (f"{message} r1 plan", "first room")
        wait_for(lambda: agent.store.pending_for_token(agent.token) is None)
        # The waiter serves r2 through a run row of its own, so r2 reports it active too.
        assert agent.join("r2", "exec")["role_statuses"] == [
            {"name": "exec", "connection": "active", "waiter": "active"},
            {"name": "plan", "connection": "active", "waiter": "offline"},
        ]
    finally:
        if process is not None:
            _finish(process)
        agent.close()
        peer.close()


def test_takeover_of_one_room_keeps_the_others(tmp_path):
    db = tmp_path / "db"
    agent, peer, other = Channel(db), Channel(db), Channel(db)
    process = None
    try:
        agent.join("r1", "exec")
        agent.join("r2", "exec")
        process = _waiter(db, agent.token)
        wait_for(lambda: agent.store.last_waiter_for_token(agent.token))

        other.join("r1", "exec")  # takes r1 away from agent
        with pytest.raises(ValueError, match="no longer owns its identity in r1"):
            agent.send("stale", to="exec", room="r1")
        with pytest.raises(ValueError, match="no longer owns"):
            agent.leave(room="r1")

        peer.join("r2", "plan")
        message = peer.send("still here", to="exec")["deliveries"][0]["message_id"]
        assert _read_delivery(process) == (f"{message} r2 plan", "still here")
        assert agent.rename("run", room="r2")["participant"]["name"] == "run"
        message = peer.send("renamed", to="run")["deliveries"][0]["message_id"]
        assert _read_delivery(process) == (f"{message} r2 plan", "renamed")
        assert process.poll() is None

        peer.join("r1", "plan")
        r1_message = peer.send("for the new owner", to="exec", room="r1")["deliveries"][0]["message_id"]
        assert agent.store.pending(other.identities["r1"]["id"])["id"] == r1_message
        wait_for(lambda: agent.store.pending_for_token(agent.token) is None)
        assert agent.store.pending(other.identities["r1"]["id"])["id"] == r1_message

        # Only r2 is still owned: it is the sole room, so room may be omitted,
        # and the waiter (whose newest run row belongs to the lost room) stays active.
        joined = agent.join("r2", "run")
        assert joined["rooms"] == ["r2"] and joined["waiter"] == "active"
        assert joined["role_statuses"][1] == {"name": "run", "connection": "active", "waiter": "active"}
        message = peer.send("no room argument", to="run", room="r2")["deliveries"][0]["message_id"]
        assert _read_delivery(process) == (f"{message} r2 plan", "no room argument")
        assert agent.send("reply", to="plan")["deliveries"] == [{"to": "plan", "message_id": message + 1}]
        with pytest.raises(ValueError, match="Not joined to r3; joined: r2"):
            agent.send("x", room="r3")
        with pytest.raises(ValueError, match="no longer owns its identity in r1"):
            agent.join("r1", "other")
    finally:
        if process is not None:
            _finish(process)
        agent.close()
        peer.close()
        other.close()


def test_room_argument_is_required_only_with_several_rooms(tmp_path):
    agent = Channel(tmp_path / "db")
    peer = Channel(tmp_path / "db")
    try:
        with pytest.raises(ValueError, match="Call join"):
            agent.send("x")
        agent.join("r1", "exec")
        peer.join("r1", "plan")
        assert agent.send("one room", to="plan")["deliveries"][0]["to"] == "plan"
        agent.join("r2", "exec")
        for call in (
            lambda: agent.send("x", to="plan"),
            lambda: agent.rename("run"),
            lambda: agent.leave(),
        ):
            with pytest.raises(ValueError, match="room is required; joined: r1, r2"):
                call()
        with pytest.raises(ValueError, match="Not joined to r3; joined: r1, r2"):
            agent.send("x", room="r3")
        with pytest.raises(ValueError, match="Already joined r1 as exec; use rename"):
            agent.join("r1", "other")
        assert agent.leave(room="r2")["rooms"] == ["r1"]
        assert agent.send("back to one room", to="plan")["deliveries"][0]["to"] == "plan"
    finally:
        agent.close()
        peer.close()


def test_broadcast_stays_inside_its_room(tmp_path):
    db = tmp_path / "db"
    agent, one, two = Channel(db), Channel(db), Channel(db)
    try:
        agent.join("r1", "exec")
        agent.join("r2", "exec")
        one.join("r1", "plan")
        two.join("r2", "plan")
        assert [d["to"] for d in agent.send("hello r1", room="r1")["deliveries"]] == ["plan"]
        assert agent.store.pending(one.identities["r1"]["id"])["text"] == "hello r1"
        assert agent.store.pending(two.identities["r2"]["id"]) is None
    finally:
        agent.close()
        one.close()
        two.close()


def test_reconnect_recovers_pending_messages_of_every_room(tmp_path):
    db = tmp_path / "db"
    old, peer = Channel(db), Channel(db)
    old.join("r1", "exec")
    old.join("r2", "exec")
    peer.join("r1", "plan")
    peer.join("r2", "plan")
    first = peer.send("while away 1", to="exec", room="r1")["deliveries"][0]["message_id"]
    second = peer.send("while away 2", to="exec", room="r2")["deliveries"][0]["message_id"]
    old.close()

    fresh = Channel(db)
    process = None
    try:
        assert fresh.join("r1", "exec")["waiter"] == "offline"
        assert fresh.join("r2", "exec")["rooms"] == ["r1", "r2"]
        process = _waiter(db, fresh.token)
        assert _read_delivery(process) == (f"{first} r1 plan", "while away 1")
        assert _read_delivery(process) == (f"{second} r2 plan", "while away 2")
        wait_for(lambda: fresh.store.pending_for_token(fresh.token) is None)
    finally:
        if process is not None:
            _finish(process)
        fresh.close()
        peer.close()


def test_last_leave_and_full_takeover_issue_a_fresh_token(tmp_path):
    db = tmp_path / "db"
    agent, other = Channel(db), Channel(db)
    try:
        agent.join("r1", "exec", "claude-code")
        first_token, first_script = agent.token, agent.script
        agent.join("r2", "exec", "claude-code")
        left = agent.leave(room="r1")
        assert left["left"]["name"] == "exec" and left["rooms"] == ["r2"]
        assert (agent.token, agent.script) == (first_token, first_script)
        assert agent.leave(room="r2")["rooms"] == []
        assert agent.token is None and agent.script is None
        assert not first_script.exists()

        rejoined = agent.join("r3", "exec", "claude-code")
        assert rejoined["waiter"] == "offline"
        assert agent.token != first_token and agent.script != first_script
        second_token, second_script = agent.token, agent.script

        other.join("r3", "exec")  # every room of agent is now owned elsewhere
        assert not agent.store.token_active(second_token)
        with agent.store.db:  # a registration the supervisor has not taken yet
            agent.store.db.execute(
                "INSERT INTO waiter_requests VALUES (?, 'thread-9', 1, NULL)", (second_token,)
            )
        assert agent.join("r4", "exec", "claude-code")["rooms"] == ["r4"]
        assert agent.token != second_token and agent.script != second_script
        assert not second_script.exists()
        assert agent.store.waiter_request(second_token) is None
        assert agent.store.token_active(agent.token)
    finally:
        agent.close()
        other.close()


def test_open_run_of_another_room_wins_over_a_newer_closed_one(tmp_path):
    store = Store(tmp_path / "db")
    try:
        first = store.participant("r1", "exec")
        second = store.participant("r2", "exec")
        store.activate(first["id"], "old")
        store.activate(second["id"], "old")
        kept = store.waiter_started(first["id"], "old", 100)
        store.waiter_started(second["id"], "old", 100)
        store.activate(second["id"], "new")  # r2 taken over; its old run is closed
        store.waiter_started(second["id"], "new", 200)
        closed = store.last_waiter(second["id"], "old")
        assert closed["exit_kind"] == "disappeared"
        assert store.last_waiter_for_token("old")["id"] == kept
        assert store.role_statuses("r1")[0]["waiter"] == "active"
        with store.db:  # the takeover closed r2's row a second before the waiter exits
            store.db.execute("UPDATE waiter_runs SET ended_at=ended_at-1 WHERE id=?", (closed["id"],))
        store.waiter_finished("old", "signal", 143, "SIGTERM")
        assert store.last_waiter_for_token("old")["exit_kind"] == "signal"
    finally:
        store.close()


def test_replacement_waiter_after_abrupt_exit_records_its_own_run(tmp_path):
    db = tmp_path / "db"
    agent = Channel(db)
    replacement = None
    try:
        participant = agent.join("r1", "exec")["participant"]
        agent.join("r2", "exec")
        killed = _waiter(db, agent.token)
        first_run = wait_for(lambda: agent.store.last_waiter(participant["id"], agent.token))
        killed.kill()
        killed.wait(timeout=2)
        _finish(killed)
        assert agent.store.last_waiter_for_token(agent.token)["ended_at"] is None

        replacement = _waiter(db, agent.token)
        wait_for(lambda: agent.store.last_waiter(participant["id"], agent.token)["id"] > first_run["id"])
        current = agent.store.last_waiter(participant["id"], agent.token)
        assert current["pid"] == replacement.pid and current["ended_at"] is None
        old = dict(agent.store.db.execute(
            "SELECT exit_kind, ended_at FROM waiter_runs WHERE id=?", (first_run["id"],)
        ).fetchone())
        assert old["exit_kind"] == "disappeared" and old["ended_at"] is not None
        wait_for(lambda: agent.store.db.execute(  # the second room's row follows the first
            "SELECT count(*) FROM waiter_runs WHERE token=? AND ended_at IS NULL", (agent.token,)
        ).fetchone()[0] == 2)
    finally:
        if replacement is not None:
            _finish(replacement)
        agent.close()


def test_room_and_name_reject_whitespace(tmp_path):
    store = Store(tmp_path / "db")
    try:
        for room, name in (("a b", "x"), ("a", "x y"), ("a\n", "x"), ("", "x"), ("a", "\t"),
                           ("a" * 201, "x"), ("a", "😀" * 101)):
            with pytest.raises(ValueError, match="1-200 UTF-16 code units with no whitespace"):
                store.participant(room, name)
        assert store.participant("a" * 200, "😀" * 100)["name"] == "😀" * 100
        participant = store.participant("a", "x")
        with pytest.raises(ValueError, match="no whitespace"):
            store.rename(participant["id"], "x y")
        with pytest.raises(ValueError, match="1-200"):
            store.rename(participant["id"], "b" * 201)
    finally:
        store.close()


def test_a_room_keeps_its_policy_and_join_always_reports_it(tmp_path):
    db = tmp_path / "db"
    first, second = Channel(db), Channel(db)
    try:
        assert first.join("r1", "plan")["policy"] is None          # a room with none says so
        rules = "Report finished work or real blockers only.\nSend to one role; broadcast only notices."
        assert first.join("r1", "plan", policy=rules)["policy"] == rules
        assert second.join("r1", "exec")["policy"] == rules        # every joiner, no policy given
        assert first.join("r2", "plan")["policy"] is None          # per room
        assert first.join("r1", "plan", policy="  ")["policy"] is None   # blank clears
        first.join("r1", "plan", policy=rules)
    finally:
        first.close()
        second.close()
    assert Channel(db).store.policy("r1") == rules                # durable across connections


def test_policy_column_is_added_to_an_existing_store(tmp_path):
    path = tmp_path / "db"
    db = sqlite3.connect(path)
    db.executescript("""
        CREATE TABLE rooms (name TEXT PRIMARY KEY, last_activity INTEGER NOT NULL);
        INSERT INTO rooms VALUES ('old', 1);
        CREATE TABLE participants (
            id TEXT PRIMARY KEY, room TEXT NOT NULL, name TEXT NOT NULL,
            token TEXT, left_at INTEGER, UNIQUE(room, name)
        );
        INSERT INTO participants(id, room, name) VALUES ('p1', 'old', 'plan');
    """)
    db.close()
    store = Store(path)
    try:
        assert store.policy("old") is None
        assert store.registered_roles("old") == ["plan"]
        store.set_policy("old", "rules")
        assert store.policy("old") == "rules"
    finally:
        store.close()
