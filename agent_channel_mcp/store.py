import sqlite3
import uuid
from pathlib import Path


class Store:
    def __init__(self, path: Path):
        self.path = path.expanduser().resolve()
        self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.db = sqlite3.connect(self.path, timeout=10)
        self.path.chmod(0o600)
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA foreign_keys = ON")
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS participants (
                id TEXT PRIMARY KEY, room TEXT NOT NULL, name TEXT NOT NULL,
                token TEXT, UNIQUE(room, name)
            );
            CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sender_id TEXT NOT NULL REFERENCES participants(id),
                recipient_id TEXT NOT NULL REFERENCES participants(id),
                text TEXT NOT NULL, acknowledged INTEGER NOT NULL DEFAULT 0
            );
            CREATE INDEX IF NOT EXISTS inbox
                ON messages(recipient_id, acknowledged, id);
        """)

    def participant(self, room: str, name: str) -> dict:
        if not room.strip() or not name.strip():
            raise ValueError("room and name must not be blank")
        with self.db:
            self.db.execute(
                "INSERT INTO participants(id, room, name) VALUES (?, ?, ?) "
                "ON CONFLICT(room, name) DO NOTHING", (uuid.uuid4().hex, room, name)
            )
        return dict(self.db.execute(
            "SELECT id, room, name FROM participants WHERE room=? AND name=?", (room, name)
        ).fetchone())

    def participants(self, room: str) -> list[dict]:
        return [dict(row) for row in self.db.execute(
            "SELECT id, room, name FROM participants WHERE room=? ORDER BY name", (room,)
        )]

    def send(self, sender_id: str, to: str, text: str) -> int:
        if not text.strip():
            raise ValueError("text must not be blank")
        with self.db:
            target = self.db.execute(
                "SELECT recipient.id FROM participants sender JOIN participants recipient "
                "ON sender.room=recipient.room WHERE sender.id=? AND recipient.name=?",
                (sender_id, to)
            ).fetchone()
            if target is None:
                raise ValueError("Recipient has not joined this room; call join() again to refresh participants")
            cursor = self.db.execute(
                "INSERT INTO messages(sender_id, recipient_id, text) VALUES (?, ?, ?)",
                (sender_id, target["id"], text)
            )
            return cursor.lastrowid

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
            self.db.execute("UPDATE participants SET token=? WHERE id=?", (token, participant_id))

    def deactivate(self, participant_id: str, token: str) -> None:
        with self.db:
            self.db.execute(
                "UPDATE participants SET token=NULL WHERE id=? AND token=?", (participant_id, token)
            )

    def is_active(self, participant_id: str, token: str) -> bool:
        return self.db.execute(
            "SELECT 1 FROM participants WHERE id=? AND token=?", (participant_id, token)
        ).fetchone() is not None

    def close(self) -> None:
        self.db.close()
