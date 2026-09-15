import os
import re
import sqlite3
import stat
import time
import uuid
from pathlib import Path


ROOM_TTL_SECONDS = 12 * 60 * 60
HEARTBEAT_INTERVAL_SECONDS = 5
PRESENCE_LEASE_SECONDS = 15
# Bumped when the tables or columns change; a store at this version skips the setup
# transaction, so opening a connection takes no write lock.
# 2: meta(channel_id) and messages.attempted, for delivery keys a receiver can recognise.
SCHEMA_VERSION = 2
# A token names presence files beside the database; it is a file-name component.
TOKEN_PATTERN = re.compile(r"[A-Za-z0-9_-]{1,64}")


def is_busy(error: sqlite3.Error) -> bool:
    """SQLITE_BUSY and its extended codes (BUSY_SNAPSHOT, BUSY_RECOVERY): another
    connection holds the database for now. Python 3.11 carries the code on the
    error; 3.10 leaves only SQLite's fixed message for that code."""
    code = getattr(error, "sqlite_errorcode", None)
    if code is not None:
        return code & 0xFF == 5   # SQLITE_BUSY
    return str(error) == "database is locked"


class Store:
    SEND_BUSY_TIMEOUT_SECONDS = 2.0
    SEND_DEADLINE_SECONDS = 10.0

    def __init__(self, path: Path):
        self.path = path.expanduser().resolve()
        self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.db = sqlite3.connect(self.path, timeout=10)
        # Whatever fails from here on, the connection does not outlive the failure.
        try:
            self.path.chmod(0o600)
            self.db.row_factory = sqlite3.Row
            self.db.execute("PRAGMA foreign_keys = ON")
            # Readers and the writer no longer block each other between processes.
            self.db.execute("PRAGMA journal_mode = WAL")
            # A commit holds the one write lock until its fsync returns, and on a loaded
            # disk that is seconds: with a heartbeat committed every few seconds by each
            # server and waiter, the lock was held nearly all the time and a send found
            # it taken for longer than any wait. In WAL mode NORMAL fsyncs at checkpoints
            # instead, outside the write lock; a process crash loses nothing, only a
            # power loss can drop the last commits, and a message channel can bear that.
            self.db.execute("PRAGMA synchronous = NORMAL")
            # A store already at this code's schema version is opened with reads only:
            # the setup below is skipped, and with it the write lock every connection
            # used to take. A newer version is refused; an older one is brought up to
            # date under the write lock, re-reading the version there in case another
            # process did it first.
            version = self.db.execute("PRAGMA user_version").fetchone()[0]
            if version > SCHEMA_VERSION:
                raise RuntimeError(
                    f"{self.path} has schema version {version}; this code knows {SCHEMA_VERSION}"
                )
            if version == SCHEMA_VERSION:
                return
            self.db.execute("BEGIN IMMEDIATE")
            version = self.db.execute("PRAGMA user_version").fetchone()[0]
            if version > SCHEMA_VERSION:              # raised meanwhile by newer code
                raise RuntimeError(
                    f"{self.path} has schema version {version}; this code knows {SCHEMA_VERSION}"
                )
            if version == SCHEMA_VERSION:
                self.db.rollback()
                return
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
                "token TEXT PRIMARY KEY, codex_thread TEXT NOT NULL, requested_at INTEGER NOT NULL, "
                "request_id TEXT)"
            )
            request_columns = {
                row["name"] for row in self.db.execute("PRAGMA table_info(waiter_requests)")
            }
            if "participant_id" in request_columns:
                # Requests were keyed by participant; one connection now holds one request.
                self.db.execute(
                    "CREATE TABLE waiter_requests_by_token ("
                    "token TEXT PRIMARY KEY, codex_thread TEXT NOT NULL, requested_at INTEGER NOT NULL, "
                    "request_id TEXT)"
                )
                self.db.execute(
                    "INSERT INTO waiter_requests_by_token(token, codex_thread, requested_at) "
                    "SELECT token, codex_thread, MAX(requested_at) FROM waiter_requests GROUP BY token"
                )
                self.db.execute("DROP TABLE waiter_requests")
                self.db.execute("ALTER TABLE waiter_requests_by_token RENAME TO waiter_requests")
            elif "request_id" not in request_columns:
                # A request is told from the next one by its own id, not by its second.
                self.db.execute("ALTER TABLE waiter_requests ADD COLUMN request_id TEXT")
            self.db.execute(
                "CREATE INDEX IF NOT EXISTS inbox "
                "ON messages(recipient_id, acknowledged, id)"
            )
            self.db.execute(
                "CREATE INDEX IF NOT EXISTS waiter_history "
                "ON waiter_runs(participant_id, id)"
            )
            if "policy" not in {
                row["name"] for row in self.db.execute("PRAGMA table_info(rooms)")
            }:
                self.db.execute("ALTER TABLE rooms ADD COLUMN policy TEXT")
            columns = {
                row["name"] for row in self.db.execute("PRAGMA table_info(participants)")
            }
            if "left_at" not in columns:
                self.db.execute("ALTER TABLE participants ADD COLUMN left_at INTEGER")
            if "connection_heartbeat_at" not in columns:
                self.db.execute(
                    "ALTER TABLE participants ADD COLUMN connection_heartbeat_at INTEGER"
                )
            # A message's delivery to a receiver that keeps what it accepted is keyed by
            # this store's id and the message's; `attempted` says a delivery of it may
            # already have reached the receiver, so the next attempt looks before adding.
            if "attempted" not in {
                row["name"] for row in self.db.execute("PRAGMA table_info(messages)")
            }:
                self.db.execute(
                    "ALTER TABLE messages ADD COLUMN attempted INTEGER NOT NULL DEFAULT 0"
                )
            self.db.execute(
                "CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)"
            )
            self.db.execute(
                "INSERT OR IGNORE INTO meta(key, value) VALUES ('channel_id', ?)",
                (uuid.uuid4().hex,),
            )
            self.db.execute(
                "INSERT OR IGNORE INTO rooms(name, last_activity) "
                "SELECT DISTINCT room, ? FROM participants", (int(time.time()),)
            )
            self.db.execute(f"PRAGMA user_version = {SCHEMA_VERSION}")
            self.db.commit()
        except Exception:
            self.db.rollback()
            self.db.close()
            raise

    # Presence lives in files beside the database, one per connection token and one per
    # waiter token (the waiter's lock file), refreshed by touching their mtime: the
    # heartbeat that every connection and waiter repeats never takes the database's
    # write lock. Presence columns stay in the tables and are still written by the
    # events that already write (join, waiter start, errors, exits), and by servers
    # of the previous version; a reader takes the later of file and column for the
    # same token, so an old process is seen while it runs, and a token's file can
    # never vouch for another token. Touching a file is a write to the same disk and
    # can itself wait on it; nothing here claims presence is immune to that.

    def _presence_path(self, kind: str, token: str) -> Path:
        if not TOKEN_PATTERN.fullmatch(token):
            raise ValueError("token must be 1-64 characters of [A-Za-z0-9_-]")
        suffix = "conn" if kind == "connection" else "wait.lock"
        return self.path.with_name(f"{self.path.name}.{token}.{suffix}")

    @staticmethod
    def _presence_fd(path: Path, flags: int) -> int:
        """The file itself, never a link to one and never a special file: opened
        without following symlinks and checked after opening, so the file that is
        touched or read is the one that was checked."""
        # Non-blocking, so a FIFO left at the path cannot hold the open until a peer
        # appears; the flag changes nothing for the regular file this must be.
        fd = os.open(path, flags | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK, 0o600)
        try:
            if not stat.S_ISREG(os.fstat(fd).st_mode):
                raise OSError(f"{path} is not a regular file")
        except BaseException:
            os.close(fd)
            raise
        return fd

    def _presence_touch(self, kind: str, token: str) -> None:
        fd = self._presence_fd(self._presence_path(kind, token), os.O_WRONLY | os.O_CREAT)
        try:
            os.utime(fd, None)
        finally:
            os.close(fd)

    def _presence_mtime(self, kind: str, token: str) -> int | None:
        """None when there is no such file, or when what is there is not a plain
        file of ours: an unreadable presence says nothing."""
        try:
            fd = self._presence_fd(self._presence_path(kind, token), os.O_RDONLY)
        except (OSError, ValueError):
            return None
        try:
            return int(os.fstat(fd).st_mtime)
        finally:
            os.close(fd)

    @staticmethod
    def _later(column: int | None, mtime: int | None) -> tuple[int | None, str | None]:
        """The later of the two presence readings and where it came from."""
        if mtime is not None and (column is None or mtime >= column):
            return mtime, "file"
        if column is not None:
            return column, "db"
        return None, None

    def connection_seen_at(self, token: str) -> int | None:
        """When this connection was last seen: its presence file, or the column a
        previous-version server writes, whichever is later."""
        row = self.db.execute(
            "SELECT max(connection_heartbeat_at) AS at FROM participants "
            "WHERE token=? AND left_at IS NULL", (token,)
        ).fetchone()
        return self._later(row["at"], self._presence_mtime("connection", token))[0]

    def _touch(self, room: str, now: int) -> None:
        self.db.execute(
            "INSERT INTO rooms(name, last_activity) VALUES (?, ?) "
            "ON CONFLICT(name) DO UPDATE SET last_activity=excluded.last_activity",
            (room, now),
        )

    def set_policy(self, room: str, policy: str) -> None:
        """Keep the room's standing rules; an empty text clears them."""
        with self.db:
            self._touch(room, int(time.time()))
            self.db.execute("UPDATE rooms SET policy=? WHERE name=?",
                            (policy.strip() or None, room))

    def policy(self, room: str) -> str | None:
        row = self.db.execute("SELECT policy FROM rooms WHERE name=?", (room,)).fetchone()
        return None if row is None else row["policy"]

    def _collect_garbage(self, now: int) -> list[str]:
        cutoff = now - ROOM_TTL_SECONDS
        lease = now - PRESENCE_LEASE_SECONDS
        rooms = []
        for row in self.db.execute(
            "SELECT r.name FROM rooms r WHERE r.last_activity<=? "
            "AND NOT EXISTS ("
            "  SELECT 1 FROM messages m JOIN participants p ON p.id=m.recipient_id "
            "  WHERE p.room=r.name AND m.acknowledged=0"
            ") ORDER BY r.name",
            (cutoff,),
        ).fetchall():
            tokens = [t["token"] for t in self.db.execute(
                "SELECT DISTINCT token FROM participants WHERE room=? AND left_at IS NULL "
                "AND token IS NOT NULL", (row["name"],)
            )]
            if not any((self.connection_seen_at(token) or 0) >= lease for token in tokens):
                rooms.append(row["name"])
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

    @staticmethod
    def _check_label(kind: str, value: str) -> None:
        # Delivery headers are "id room sender" on one line: no whitespace, and short
        # enough that the header never reaches the 500-unit wrap.
        units = len(value.encode("utf-16-le")) // 2
        if not 1 <= units <= 200 or any(char.isspace() for char in value):
            raise ValueError(f"{kind} must be 1-200 UTF-16 code units with no whitespace")

    def participant(self, room: str, name: str) -> dict:
        self._check_label("room", room)
        self._check_label("name", name)
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

    def registered_roles(self, room: str) -> list[str]:
        return [row["name"] for row in self.db.execute(
            "SELECT name FROM participants "
            "WHERE room=? AND left_at IS NULL ORDER BY name", (room,)
        )]

    def role_statuses(self, room: str, now: int | None = None) -> list[dict]:
        current = int(time.time()) if now is None else now
        cutoff = current - PRESENCE_LEASE_SECONDS
        statuses = []
        for participant in self.db.execute(
            "SELECT id, name, token FROM participants "
            "WHERE room=? AND left_at IS NULL ORDER BY name", (room,)
        ).fetchall():
            present = participant["token"] is not None
            connection_active = (
                present
                and (self.connection_seen_at(participant["token"]) or 0) >= cutoff
            )
            waiter_active = False
            if present:
                waiter = self.last_waiter_for_token(participant["token"])
                waiter_active = (
                    waiter is not None
                    and waiter["ended_at"] is None
                    and waiter["heartbeat_at"] >= cutoff
                )
            statuses.append({
                "name": participant["name"],
                "connection": "active" if connection_active else "offline",
                "waiter": "active" if waiter_active else "offline",
            })
        return statuses

    def send(self, sender_id: str, text: str, to: str | None = None,
             token: str | None = None) -> list[dict]:
        """token, when given, must still own the sender inside the same transaction
        as the write: a session taken over between its check and its write sends
        nothing. Each attempt checks again."""
        if not text.strip():
            raise ValueError("text must not be blank")
        # Another process (a waiter acknowledging, another sender) may hold the write
        # lock for a moment. Each attempt waits SEND_BUSY_TIMEOUT_SECONDS for it, then
        # is rolled back whole and repeated after a short pause, until the deadline;
        # every other error is raised as it comes. A message is inserted by exactly
        # one committed attempt.
        started = time.monotonic()
        pause = 0.02
        self.db.execute(f"PRAGMA busy_timeout = {int(self.SEND_BUSY_TIMEOUT_SECONDS * 1000)}")
        try:
            while True:
                try:
                    return self._send_once(sender_id, text, to, token)
                except sqlite3.OperationalError as error:
                    if not is_busy(error):
                        raise
                    remaining = self.SEND_DEADLINE_SECONDS - (time.monotonic() - started)
                    if remaining < pause + self.SEND_BUSY_TIMEOUT_SECONDS:
                        raise
                    time.sleep(pause)
                    pause = min(pause * 2, 0.5)
        finally:
            self.db.execute("PRAGMA busy_timeout = 10000")

    def _send_once(self, sender_id: str, text: str, to: str | None,
                   token: str | None) -> list[dict]:
        now = int(time.time())
        with self.db:
            # Take the write lock before reading, so the rows read and the rows
            # written belong to one snapshot: no read that must later be promoted
            # to a write past a commit another process made in between.
            self.db.execute("BEGIN IMMEDIATE")
            sender = self.db.execute(
                "SELECT room, token FROM participants WHERE id=? AND left_at IS NULL",
                (sender_id,),
            ).fetchone()
            if sender is None:
                raise ValueError("Sender has left the room")
            if token is not None and sender["token"] != token:
                raise ValueError("This MCP session no longer owns its identity")
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
                    raise ValueError(
                        "Recipient is not registered in this room; call join() again "
                        "to refresh registered roles and live status"
                    )
            deliveries = []
            for recipient in recipients:
                cursor = self.db.execute(
                    "INSERT INTO messages(sender_id, recipient_id, text) VALUES (?, ?, ?)",
                    (sender_id, recipient["id"], text),
                )
                deliveries.append({"to": recipient["name"], "message_id": cursor.lastrowid})
            self._touch(sender["room"], now)
            return deliveries

    def rename(self, participant_id: str, name: str, token: str | None = None) -> dict:
        """token, when given, must still own the participant in the transaction that
        renames it."""
        self._check_label("name", name)
        now = int(time.time())
        try:
            with self.db:
                self.db.execute("BEGIN IMMEDIATE")
                participant = self.db.execute(
                    "SELECT room, token FROM participants WHERE id=? AND left_at IS NULL",
                    (participant_id,),
                ).fetchone()
                if participant is None:
                    raise ValueError("Participant has left the room")
                if token is not None and participant["token"] != token:
                    raise ValueError("This MCP session no longer owns its identity")
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
            self.db.execute("BEGIN IMMEDIATE")
            participant = self.db.execute(
                "SELECT id, room, name FROM participants "
                "WHERE id=? AND token=? AND left_at IS NULL", (participant_id, token)
            ).fetchone()
            if participant is None:
                raise ValueError("This MCP session no longer owns its identity")
            self.db.execute(
                "UPDATE participants SET token=NULL, connection_heartbeat_at=NULL, left_at=? "
                "WHERE id=? AND token=?",
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

    def pending_for_token(self, token: str, excluding: set[int] = frozenset()) -> dict | None:
        """Oldest unacknowledged message across every room this connection holds,
        leaving out the ids in `excluding` (ones the waiter has given up on)."""
        skipped = sorted(excluding)
        row = self.db.execute(
            "SELECT m.id, s.room, s.name AS sender, r.id AS recipient_id, r.name AS recipient, "
            "m.text, m.attempted "
            "FROM messages m JOIN participants s ON s.id=m.sender_id "
            "JOIN participants r ON r.id=m.recipient_id "
            "WHERE r.token=? AND r.left_at IS NULL AND m.acknowledged=0 "
            + ("AND m.id NOT IN (" + ",".join("?" * len(skipped)) + ") " if skipped else "")
            + "ORDER BY m.id LIMIT 1",
            (token, *skipped),
        ).fetchone()
        return dict(row) if row is not None else None

    @property
    def channel_id(self) -> str:
        """This store's own id, part of every delivery key it hands a receiver."""
        return self.db.execute("SELECT value FROM meta WHERE key='channel_id'").fetchone()["value"]

    def mark_attempted(self, message_id: int, token: str) -> bool:
        """Written before a delivery is handed to a receiver, so that a crash or a lost
        answer after the hand-over leaves the message marked: a marked message is only
        ever looked for at the receiver, never added again. Written only while this
        connection still owns the recipient, in the same statement that checks it:
        False when it does not, and then nothing is handed over."""
        with self.db:
            cursor = self.db.execute(
                "UPDATE messages SET attempted=1 WHERE id=? AND recipient_id IN "
                "(SELECT id FROM participants WHERE token=? AND left_at IS NULL)",
                (message_id, token),
            )
        return cursor.rowcount == 1

    def attempted(self, message_id: int) -> bool:
        """Whether a delivery of the message may already have reached its receiver;
        read under the delivery lock, since another waiter may have marked it."""
        row = self.db.execute("SELECT attempted FROM messages WHERE id=?", (message_id,)).fetchone()
        return bool(row and row["attempted"])

    def clear_attempted(self, message_id: int) -> None:
        """When the receiver answered that it did not take the message: the hand-over
        is known not to have happened, so the next attempt may add."""
        with self.db:
            self.db.execute("UPDATE messages SET attempted=0 WHERE id=?", (message_id,))

    def ack(self, participant_id: str, message_id: int) -> None:
        with self.db:
            self.db.execute(
                "UPDATE messages SET acknowledged=1 WHERE id=? AND recipient_id=?",
                (message_id, participant_id)
            )

    def ack_for_token(self, token: str, message_id: int) -> bool:
        """Acknowledge only while this connection still owns the recipient."""
        with self.db:
            cursor = self.db.execute(
                "UPDATE messages SET acknowledged=1 WHERE id=? AND recipient_id IN "
                "(SELECT id FROM participants WHERE token=? AND left_at IS NULL)",
                (message_id, token),
            )
        return cursor.rowcount == 1

    def activate(self, participant_id: str, token: str) -> None:
        now = int(time.time())
        self._presence_path("connection", token)          # a token that can name a file
        with self.db:
            self.db.execute(
                "UPDATE participants SET token=?, connection_heartbeat_at=? "
                "WHERE id=? AND left_at IS NULL",
                (token, now, participant_id),
            )
        self._presence_touch("connection", token)

    def connection_heartbeat(self, token: str) -> bool:
        """Refresh the connection's presence file; False once the token owns nothing."""
        if not self.token_active(token):
            return False
        self._presence_touch("connection", token)
        return True

    def deactivate(self, token: str) -> None:
        with self.db:
            self.db.execute("DELETE FROM waiter_requests WHERE token=?", (token,))
            self.db.execute(
                "UPDATE participants SET token=NULL, connection_heartbeat_at=NULL "
                "WHERE token=?", (token,)
            )
        try:
            self._presence_path("connection", token).unlink(missing_ok=True)
        except ValueError:
            pass

    def is_active(self, participant_id: str, token: str) -> bool:
        return self.db.execute(
            "SELECT 1 FROM participants WHERE id=? AND token=? AND left_at IS NULL",
            (participant_id, token),
        ).fetchone() is not None

    def token_active(self, token: str) -> bool:
        return self.db.execute(
            "SELECT 1 FROM participants WHERE token=? AND left_at IS NULL LIMIT 1", (token,)
        ).fetchone() is not None

    def token_participants(self, token: str, without_run: bool = False) -> list[str]:
        """Participants of this connection; with without_run, only those lacking an
        open waiter_runs row."""
        query = "SELECT p.id FROM participants p WHERE p.token=? AND p.left_at IS NULL"
        values: tuple = (token,)
        if without_run:
            query += (" AND NOT EXISTS (SELECT 1 FROM waiter_runs w WHERE w.participant_id=p.id "
                      "AND w.token=? AND w.ended_at IS NULL)")
            values += (token,)
        return [row["id"] for row in self.db.execute(query + " ORDER BY p.room", values)]

    def waiter_started(self, participant_id: str, token: str | None, pid: int) -> int | None:
        """Open this waiter's run for the participant, closing the runs before it.
        A waiter whose token no longer owns the participant, taken over since it
        read its rooms, opens nothing and closes nothing: None."""
        now = int(time.time())
        with self.db:
            self.db.execute("BEGIN IMMEDIATE")
            owner = self.db.execute(
                "SELECT token FROM participants WHERE id=? AND left_at IS NULL", (participant_id,)
            ).fetchone()
            if owner is None or (token is not None and owner["token"] != token):
                return None
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

    def waiter_heartbeat(self, token: str) -> None:
        """Refresh the waiter's presence: the mtime of its lock file, no database write."""
        self._presence_touch("waiter", token)

    def waiter_error(self, token: str, detail: str) -> None:
        now = int(time.time())
        with self.db:
            self.db.execute(
                "UPDATE waiter_runs SET heartbeat_at=?, last_error=?, last_error_at=? "
                "WHERE token=? AND ended_at IS NULL",
                (now, detail, now, token),
            )

    def waiter_finished(
        self,
        token: str,
        exit_kind: str,
        exit_code: int,
        detail: str,
    ) -> None:
        now = int(time.time())
        with self.db:
            self.db.execute(
                "UPDATE waiter_runs SET heartbeat_at=?, ended_at=?, exit_kind=?, "
                "exit_code=?, detail=? WHERE token=? AND ended_at IS NULL",
                (now, now, exit_kind, exit_code, detail, token),
            )

    _WAITER_COLUMNS = (
        "SELECT id, token, pid, started_at, heartbeat_at, ended_at, exit_kind, exit_code, "
        "detail, last_error, last_error_at FROM waiter_runs WHERE "
    )

    def _waiter_run(self, row) -> dict | None:
        """A run row with heartbeat_at raised to the mtime of the waiter's lock file
        while the run is open; heartbeat_source says which reading it is."""
        if row is None:
            return None
        run = dict(row)
        source = "db"
        if run["ended_at"] is None and run["token"] is not None:
            run["heartbeat_at"], source = self._later(
                run["heartbeat_at"], self._presence_mtime("waiter", run["token"])
            )
        run["heartbeat_source"] = source
        return run

    def last_waiter(self, participant_id: str, token: str | None = None) -> dict | None:
        query = self._WAITER_COLUMNS + "participant_id=?"
        values: tuple = (participant_id,)
        if token is not None:
            query += " AND token=?"
            values += (token,)
        return self._waiter_run(
            self.db.execute(query + " ORDER BY id DESC LIMIT 1", values).fetchone()
        )

    def last_waiter_for_token(self, token: str) -> dict | None:
        """An open run of this connection if any (a taken-over room closes only its
        own row), otherwise the most recently ended one."""
        return self._waiter_run(self.db.execute(
            self._WAITER_COLUMNS + "token=? ORDER BY (ended_at IS NULL) DESC, ended_at DESC, id DESC LIMIT 1",
            (token,),
        ).fetchone())

    def request_waiter(self, token: str, codex_thread: str) -> str:
        """Ask the server for a waiter; the id returned names this request alone."""
        if not codex_thread.strip():
            raise ValueError("Codex thread ID must not be blank")
        request_id = uuid.uuid4().hex
        with self.db:
            if not self.token_active(token):
                raise ValueError("This MCP session no longer owns its identity")
            self.db.execute(
                "INSERT INTO waiter_requests(token, codex_thread, requested_at, request_id) "
                "VALUES (?, ?, ?, ?) ON CONFLICT(token) DO UPDATE SET "
                "codex_thread=excluded.codex_thread, requested_at=excluded.requested_at, "
                "request_id=excluded.request_id",
                (token, codex_thread, int(time.time()), request_id),
            )
        return request_id

    def waiter_request(self, token: str) -> dict | None:
        """The pending request, left in place until the waiter it asks for has started
        or the start has failed for good."""
        row = self.db.execute(
            "SELECT codex_thread, requested_at, request_id FROM waiter_requests WHERE token=?",
            (token,),
        ).fetchone()
        return dict(row) if row is not None else None

    def finish_waiter_request(self, token: str, request_id: str) -> None:
        """Remove that request and not a newer one made since."""
        with self.db:
            self.db.execute(
                "DELETE FROM waiter_requests WHERE token=? AND request_id IS ?",
                (token, request_id),
            )

    def latest_run_id(self, token: str) -> int:
        """The newest run row of this connection, 0 before any: a later row is a
        later attempt, whatever second either was written in."""
        row = self.db.execute(
            "SELECT max(id) FROM waiter_runs WHERE token=?", (token,)
        ).fetchone()
        return row[0] or 0

    def waiter_start_failed(self, token: str, pid: int, detail: str) -> None:
        """A waiter that never got as far as its run row: a closed row for each room
        of the connection, so join and register report the cause."""
        now = int(time.time())
        with self.db:
            for participant_id in self.token_participants(token):
                self.db.execute(
                    "INSERT INTO waiter_runs(participant_id, token, pid, started_at, "
                    "heartbeat_at, ended_at, exit_kind, exit_code, detail) "
                    "VALUES (?, ?, ?, ?, ?, ?, 'error', 1, ?)",
                    (participant_id, token, pid, now, now, now, detail),
                )

    def cancel_waiter_request(self, token: str, request_id: str | None = None) -> None:
        """Withdraw the connection's request, or only the one named."""
        with self.db:
            if request_id is None:
                self.db.execute("DELETE FROM waiter_requests WHERE token=?", (token,))
            else:
                self.db.execute(
                    "DELETE FROM waiter_requests WHERE token=? AND request_id IS ?",
                    (token, request_id),
                )

    def close(self) -> None:
        self.db.close()
