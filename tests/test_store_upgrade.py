"""Bringing a database of an earlier schema up to date: what was pending then is
marked as handed over once, what was acknowledged is left alone, and what is sent
afterwards starts unmarked."""

import sqlite3

from agent_channel_mcp.store import SCHEMA_VERSION, Store


def _database_with_history(db):
    """A store with one acknowledged and one pending message, closed."""
    store = Store(db)
    plan = store.participant("room", "plan")
    execute = store.participant("room", "exec")
    store.activate(execute["id"], "tok")
    acked = store.send(plan["id"], "done before", to="exec")[0]["message_id"]
    store.ack_for_token("tok", acked)
    legacy = store.send(plan["id"], "still pending", to="exec")[0]["message_id"]
    store.close()
    return acked, legacy


def _downgrade(db, version: int) -> None:
    """The shape a database of `version` has: without the columns and tables the
    later versions add, and every message unmarked."""
    raw = sqlite3.connect(db)
    if version < 2:
        raw.execute("ALTER TABLE messages DROP COLUMN attempted")
        raw.execute("DROP TABLE meta")
    else:
        raw.execute("UPDATE messages SET attempted=0")
    raw.execute(f"PRAGMA user_version = {version}")
    raw.commit()
    raw.close()


def _attempted(db, message_id):
    raw = sqlite3.connect(db)
    try:
        return raw.execute("SELECT acknowledged, attempted FROM messages WHERE id=?", (message_id,)).fetchone()
    finally:
        raw.close()


def test_a_version_1_database_marks_what_was_pending_and_keeps_the_rest(tmp_path):
    db = tmp_path / "channel.sqlite3"
    acked, legacy = _database_with_history(db)
    _downgrade(db, 1)
    store = Store(db)
    assert store.db.execute("PRAGMA user_version").fetchone()[0] == SCHEMA_VERSION
    assert _attempted(db, acked) == (1, 0)                    # acknowledged: untouched
    assert _attempted(db, legacy) == (0, 1)                   # pending: handed over once, maybe
    assert store.pending_for_token("tok")["id"] == legacy
    assert len(store.channel_id) == 32
    fresh = store.send(store.participant("room", "plan")["id"], "after the upgrade", to="exec")[0]["message_id"]
    assert _attempted(db, fresh) == (0, 0)                    # new: never handed over
    store.close()


def test_a_version_2_database_marks_its_pending_messages_too(tmp_path):
    db = tmp_path / "channel.sqlite3"
    acked, legacy = _database_with_history(db)
    _downgrade(db, 2)
    store = Store(db)
    assert store.db.execute("PRAGMA user_version").fetchone()[0] == SCHEMA_VERSION
    assert _attempted(db, acked) == (1, 0)
    assert _attempted(db, legacy) == (0, 1)
    store.close()


def test_a_current_database_marks_nothing_on_reopen(tmp_path):
    db = tmp_path / "channel.sqlite3"
    acked, legacy = _database_with_history(db)
    Store(db).close()
    assert _attempted(db, legacy) == (0, 0)
