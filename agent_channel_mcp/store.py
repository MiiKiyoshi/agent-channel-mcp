import sqlite3
import time
import uuid
from pathlib import Path


ROOM_TTL_SECONDS = 12 * 60 * 60


class Store:
    def __init__(self, path: Path):
        self.path = path.expanduser().resolve()
        self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.db = sqlite3.connect(self.path, timeout=10)
        self.path.chmod(0o600)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA foreign_keys = ON")
        self.db.execute("BEGIN IMMEDIATE")
        try:
            self.db.execute(
                "CREATE TABLE IF NOT EXISTS rooms "
                "(name TEXT PRIMARY KEY, last_activity INTEGER NOT NULL)"
            )
            self.db.execute(
                "CREATE TABLE IF NOT EXISTS participants ("
                "id TEXT PRIMARY KEY, room TEXT NOT NULL, name TEXT NOT NULL, "
                "token TEXT, left_at INTEGER, UNIQUE(room, name))"
            )
            self.db.execute(
                "CREATE TABLE IF NOT EXISTS messages ("
                "id INTEGER PRIMARY KEY AUTOINCREMENT, "
                "sender_id TEXT NOT NULL REFERENCES participants(id), "
                "recipient_id TEXT NOT NULL REFERENCES participants(id), "
                "text TEXT NOT NULL, acknowledged INTEGER NOT NULL DEFAULT 0)"
            )
            self.db.execute(
                "CREATE TABLE IF NOT EXISTS waiter_runs ("
                "id INTEGER PRIMARY KEY AUTOINCREMENT, "
                "participant_id TEXT NOT NULL REFERENCES participants(id) ON DELETE CASCADE, "
                "token TEXT, pid INTEGER NOT NULL, started_at INTEGER NOT NULL, "
                "heartbeat_at INTEGER NOT NULL, ended_at INTEGER, exit_kind TEXT, "
                "exit_code INTEGER, detail TEXT, last_error TEXT, last_error_at INTEGER)"
            )
            self.db.execute(
                "CREATE TABLE IF NOT EXISTS waiter_requests ("
                "participant_id TEXT PRIMARY KEY REFERENCES participants(id) ON DELETE CASCADE, "
                "token TEXT NOT NULL, codex_thread TEXT NOT NULL, requested_at INTEGER NOT NULL)"
            )
            self.db.execute(
                "CREATE INDEX IF NOT EXISTS inbox "
                "ON messages(recipient_id, acknowledged, id)"
            )
            self.db.execute(
                "CREATE INDEX IF NOT EXISTS waiter_history "
                "ON waiter_runs(participant_id, id)"
            )
            columns = {
                row["name"] for row in self.db.execute("PRAGMA table_info(participants)")
            }
            if "left_at" not in columns:
                self.db.execute("ALTER TABLE participants ADD COLUMN left_at INTEGER")
            self.db.execute(
                "INSERT OR IGNORE INTO rooms(name, last_activity) "
                "SELECT DISTINCT room, ? FROM participants", (int(time.time()),)
            )
            self.db.commit()
        except Exception:
            self.db.rollback()
            self.db.close()
            raise

    def _touch(self, room: str, now: int) -> None:
        self.db.execute(
            "INSERT INTO rooms(name, last_activity) VALUES (?, ?) "
            "ON CONFLICT(name) DO UPDATE SET last_activity=excluded.last_activity",
            (room, now),
        )

    def _collect_garbage(self, now: int) -> list[str]:
        cutoff = now - ROOM_TTL_SECONDS
        rooms = [row["name"] for row in self.db.execute(
            "SELECT r.name FROM rooms r WHERE r.last_activity<=? "
            "AND NOT EXISTS (SELECT 1 FROM participants p WHERE p.room=r.name AND p.token IS NOT NULL) "
            "AND NOT EXISTS ("
            "  SELECT 1 FROM messages m JOIN participants p ON p.id=m.recipient_id "
            "  WHERE p.room=r.name AND m.acknowledged=0"
            ") ORDER BY r.name",
            (cutoff,),
        )]
        for room in rooms:
            self.db.execute(
                "DELETE FROM messages WHERE sender_id IN "
                "(SELECT id FROM participants WHERE room=?) OR recipient_id IN "
                "(SELECT id FROM participants WHERE room=?)",
                (room, room),
            )
            self.db.execute("DELETE FROM participants WHERE room=?", (room,))
            self.db.execute("DELETE FROM rooms WHERE name=?", (room,))
        return rooms

    def participant(self, room: str, name: str) -> dict:
        if not room.strip() or not name.strip():
            raise ValueError("room and name must not be blank")
        now = int(time.time())
        with self.db:
            self._collect_garbage(now)
            self._touch(room, now)
            self.db.execute(
                "INSERT INTO participants(id, room, name, left_at) VALUES (?, ?, ?, NULL) "
                "ON CONFLICT(room, name) DO NOTHING", (uuid.uuid4().hex, room, name)
            )
            self.db.execute(
                "UPDATE participants SET left_at=NULL WHERE room=? AND name=?", (room, name)
            )
        return dict(self.db.execute(
            "SELECT id, room, name FROM participants WHERE room=? AND name=?", (room, name)
        ).fetchone())

    def participants(self, room: str) -> list[dict]:
        return [dict(row) for row in self.db.execute(
            "SELECT id, room, name FROM participants "
            "WHERE room=? AND left_at IS NULL ORDER BY name", (room,)
        )]

    def send(self, sender_id: str, text: str, to: str | None = None) -> list[dict]:
        if not text.strip():
            raise ValueError("text must not be blank")
        now = int(time.time())
        with self.db:
            sender = self.db.execute(
                "SELECT room FROM participants WHERE id=? AND left_at IS NULL", (sender_id,)
            ).fetchone()
            if sender is None:
                raise ValueError("Sender has left the room")
            if to is None:
                recipients = self.db.execute(
                    "SELECT id, name FROM participants WHERE room=? AND id<>? "
                    "AND left_at IS NULL ORDER BY name", (sender["room"], sender_id)
                ).fetchall()
            else:
                recipients = self.db.execute(
                    "SELECT id, name FROM participants WHERE room=? AND name=? "
                    "AND left_at IS NULL", (sender["room"], to)
                ).fetchall()
                if not recipients:
                    raise ValueError("Recipient has not joined this room; call join() again to refresh participants")
            deliveries = []
            for recipient in recipients:
                cursor = self.db.execute(
                    "INSERT INTO messages(sender_id, recipient_id, text) VALUES (?, ?, ?)",
                    (sender_id, recipient["id"], text),
                )
                deliveries.append({"to": recipient["name"], "message_id": cursor.lastrowid})
            self._touch(sender["room"], now)
            return deliveries

    def rename(self, participant_id: str, name: str) -> dict:
        if not name.strip():
            raise ValueError("name must not be blank")
        now = int(time.time())
        try:
            with self.db:
                participant = self.db.execute(
                    "SELECT room FROM participants WHERE id=? AND left_at IS NULL", (participant_id,)
                ).fetchone()
                if participant is None:
                    raise ValueError("Participant has left the room")
                self.db.execute("UPDATE participants SET name=? WHERE id=?", (name, participant_id))
                self._touch(participant["room"], now)
        except sqlite3.IntegrityError:
            raise ValueError("That name is already in use in this room") from None
        return dict(self.db.execute(
            "SELECT id, room, name FROM participants WHERE id=?", (participant_id,)
        ).fetchone())

    def leave(self, participant_id: str, token: str) -> dict:
        now = int(time.time())
        with self.db:
            participant = self.db.execute(
                "SELECT id, room, name FROM participants "
                "WHERE id=? AND token=? AND left_at IS NULL", (participant_id, token)
            ).fetchone()
            if participant is None:
                raise ValueError("This MCP session no longer owns its identity")
            self.db.execute(
                "DELETE FROM waiter_requests WHERE participant_id=? AND token=?",
                (participant_id, token),
            )
            self.db.execute(
                "UPDATE participants SET token=NULL, left_at=? WHERE id=? AND token=?",
                (now, participant_id, token),
            )
            self._touch(participant["room"], now)
        return dict(participant)

    def pending(self, participant_id: str) -> dict | None:
        row = self.db.execute(
            "SELECT m.id, s.room, s.name AS sender, r.name AS recipient, m.text "
            "FROM messages m JOIN participants s ON s.id=m.sender_id "
            "JOIN participants r ON r.id=m.recipient_id "
            "WHERE m.recipient_id=? AND m.acknowledged=0 ORDER BY m.id LIMIT 1",
            (participant_id,)
        ).fetchone()
        return dict(row) if row is not None else None

    def ack(self, participant_id: str, message_id: int) -> None:
        with self.db:
            self.db.execute(
                "UPDATE messages SET acknowledged=1 WHERE id=? AND recipient_id=?",
                (message_id, participant_id)
            )

    def activate(self, participant_id: str, token: str) -> None:
        with self.db:
            self.db.execute(
                "DELETE FROM waiter_requests WHERE participant_id=?", (participant_id,)
            )
            self.db.execute(
                "UPDATE participants SET token=? WHERE id=? AND left_at IS NULL",
                (token, participant_id),
            )

    def deactivate(self, participant_id: str, token: str) -> None:
        with self.db:
            self.db.execute(
                "DELETE FROM waiter_requests WHERE participant_id=? AND token=?",
                (participant_id, token),
            )
            self.db.execute(
                "UPDATE participants SET token=NULL WHERE id=? AND token=?", (participant_id, token)
            )

    def is_active(self, participant_id: str, token: str) -> bool:
        return self.db.execute(
            "SELECT 1 FROM participants WHERE id=? AND token=? AND left_at IS NULL",
            (participant_id, token),
        ).fetchone() is not None

    def waiter_started(self, participant_id: str, token: str | None, pid: int) -> int:
        now = int(time.time())
        with self.db:
            self.db.execute(
                "UPDATE waiter_runs SET ended_at=?, exit_kind='disappeared', "
                "detail='lock was free when a replacement waiter started' "
                "WHERE participant_id=? AND ended_at IS NULL",
                (now, participant_id),
            )
            cursor = self.db.execute(
                "INSERT INTO waiter_runs(participant_id, token, pid, started_at, heartbeat_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (participant_id, token, pid, now, now),
            )
        return cursor.lastrowid

    def waiter_heartbeat(self, run_id: int) -> None:
        with self.db:
            self.db.execute(
                "UPDATE waiter_runs SET heartbeat_at=? WHERE id=? AND ended_at IS NULL",
                (int(time.time()), run_id),
            )

    def waiter_error(self, run_id: int, detail: str) -> None:
        now = int(time.time())
        with self.db:
            self.db.execute(
                "UPDATE waiter_runs SET heartbeat_at=?, last_error=?, last_error_at=? "
                "WHERE id=? AND ended_at IS NULL",
                (now, detail, now, run_id),
            )

    def waiter_finished(
        self,
        run_id: int,
        exit_kind: str,
        exit_code: int,
        detail: str,
    ) -> None:
        now = int(time.time())
        with self.db:
            self.db.execute(
                "UPDATE waiter_runs SET heartbeat_at=?, ended_at=?, exit_kind=?, "
                "exit_code=?, detail=? WHERE id=? AND ended_at IS NULL",
                (now, now, exit_kind, exit_code, detail, run_id),
            )

    def last_waiter(self, participant_id: str, token: str | None = None) -> dict | None:
        query = (
            "SELECT id, pid, started_at, heartbeat_at, ended_at, exit_kind, exit_code, "
            "detail, last_error, last_error_at FROM waiter_runs WHERE participant_id=?"
        )
        values: tuple = (participant_id,)
        if token is not None:
            query += " AND token=?"
            values += (token,)
        row = self.db.execute(query + " ORDER BY id DESC LIMIT 1", values).fetchone()
        return dict(row) if row is not None else None

    def request_waiter(self, participant_id: str, token: str, codex_thread: str) -> None:
        if not codex_thread.strip():
            raise ValueError("Codex thread ID must not be blank")
        with self.db:
            if not self.is_active(participant_id, token):
                raise ValueError("This MCP session no longer owns its identity")
            self.db.execute(
                "INSERT INTO waiter_requests(participant_id, token, codex_thread, requested_at) "
                "VALUES (?, ?, ?, ?) ON CONFLICT(participant_id) DO UPDATE SET "
                "token=excluded.token, codex_thread=excluded.codex_thread, "
                "requested_at=excluded.requested_at",
                (participant_id, token, codex_thread, int(time.time())),
            )

    def take_waiter_request(self, participant_id: str, token: str) -> dict | None:
        with self.db:
            row = self.db.execute(
                "SELECT codex_thread, requested_at FROM waiter_requests "
                "WHERE participant_id=? AND token=?",
                (participant_id, token),
            ).fetchone()
            if row is not None:
                self.db.execute(
                    "DELETE FROM waiter_requests WHERE participant_id=? AND token=?",
                    (participant_id, token),
                )
        return dict(row) if row is not None else None

    def cancel_waiter_request(self, participant_id: str, token: str) -> None:
        with self.db:
            self.db.execute(
                "DELETE FROM waiter_requests WHERE participant_id=? AND token=?",
                (participant_id, token),
            )

    def close(self) -> None:
        self.db.close()
