"""Deliver messages as bounded text lines, acknowledging successful delivery."""

import argparse
import fcntl
import os
import re
import shutil
import signal
import sqlite3
import stat
import sys
import threading
import time
from pathlib import Path

from .codex_sink import (
    COMMAND as CODEX_COMMAND, CodexSink, KeyConflict, SinkError, SinkTransportError, SinkUnsupported,
)
from .store import Store, is_busy

# An app-server call that hangs is cut off after this long; the message stays
# unacknowledged and is looked for before it is added again.
CODEX_QUEUE_TIMEOUT_SECONDS = 60
CODEX_RETRY_PAUSE_SECONDS = 5
CODEX_UNSUPPORTED_PAUSE_SECONDS = 30
# A message handed over once and not found at the thread is looked for again this often.
CODEX_UNCERTAIN_PAUSE_SECONDS = 10


class DeliveryUncertain(Exception):
    """The message was handed over once, and is in neither the queue nor the items."""


class WaiterSignal(Exception):
    def __init__(self, signum: int):
        self.signum = signum


def _open_lock_file(path: Path, flags: int):
    """The lock file itself, never a link followed to another file and never a
    special file, checked on the descriptor that is then used."""
    # Non-blocking, so a FIFO left at the path cannot hold the open until a peer
    # appears; once the file is known to be regular the flag is dropped again.
    fd = os.open(path, flags | os.O_NOFOLLOW | os.O_CLOEXEC | os.O_NONBLOCK, 0o600)
    try:
        if not stat.S_ISREG(os.fstat(fd).st_mode):
            raise OSError(f"{path} is not a regular file")
        fcntl.fcntl(fd, fcntl.F_SETFL, fcntl.fcntl(fd, fcntl.F_GETFL) & ~os.O_NONBLOCK)
    except BaseException:
        os.close(fd)
        raise
    return os.fdopen(fd, "r+")


def lock(path: Path, owner: str):
    handle = _open_lock_file(path, os.O_RDWR | os.O_CREAT)
    try:
        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        handle.close()
        raise ValueError(f"Already running: {path.name}") from None
    handle.seek(0)
    handle.truncate()
    handle.write(owner)
    handle.flush()
    return handle


def lock_path(db: Path, token: str) -> Path:
    return db.with_name(f"{db.name}.{token}.wait.lock")


def render_message(message: dict) -> str:
    output = []
    text = f"{message['id']} {message['room']} {message['sender']}\n{message['text']}"
    for line in text.split("\n"):
        start = width = 0
        for index, char in enumerate(line):
            # Count UTF-16 units so a non-BMP character cannot straddle the limit.
            size = 2 if ord(char) > 0xFFFF else 1
            if width + size > 500:
                output.append(line[start:index])
                start, width = index, 0
            width += size
        output.append(line[start:])
    return "\n".join(output)


def delivery_lock(db: Path, thread_id: str):
    """The lock every waiter of this database takes around one look-then-add on a
    receiving thread, so two of them (an old one still running, a new one after a
    takeover) cannot both find nothing and both add. Released when the handle closes."""
    safe = re.sub(r"[^A-Za-z0-9_-]", "_", thread_id)[:80]
    handle = _open_lock_file(db.with_name(f"{db.name}.{safe}.deliver.lock"), os.O_RDWR | os.O_CREAT)
    fcntl.flock(handle, fcntl.LOCK_EX)
    return handle


def deliver_to_codex(store: Store, sink: CodexSink, thread_id: str, message: dict, text: str) -> str:
    """Put one message on its thread at most once: "added", or "queued"/"consumed"
    when a delivery under its key is already there. Raises KeyConflict, DeliveryUncertain,
    SinkUnsupported, SinkTransportError or SinkError, each leaving the message
    unacknowledged.

    The message is marked attempted, durably, before it is handed over, and a marked
    message is never added again: the thread's queue and items are read for its key,
    and a message in neither is uncertain, since the app-server deletes a queued
    message when its turn starts and records the item only later. The mark is cleared
    when the app-server answers that it did not take the message; a hand-over with no
    answer keeps it."""
    key = f"agent-channel:{store.channel_id}:{message['id']}"
    handle = delivery_lock(store.path, thread_id)
    try:
        # Read under the lock: another waiter may have marked and added it meanwhile.
        if store.attempted(message["id"]):
            found = sink.find(thread_id, key, text)
            if found is None:
                raise DeliveryUncertain(f"delivery uncertain for message {message['id']}; not re-adding")
            return found
        store.mark_attempted(message["id"])
        try:
            sink.add(thread_id, key, text)
        except SinkError:                        # answered: the message was not taken
            store.clear_attempted(message["id"])
            raise
        return "added"
    finally:
        handle.close()


def run(db: Path, token: str, codex_thread: str | None = None,
        stop: threading.Event | None = None) -> int:
    """Serve every room the connection identified by `token` has joined, until the
    token is inactive or `stop` is set (the server closing over a waiter it runs)."""
    stop = threading.Event() if stop is None else stop
    store = Store(db)
    try:
        with lock(lock_path(store.path, token), owner=token):
            # A replacement after an abrupt exit closes the rows the old process left open.
            for participant_id in store.token_participants(token):
                store.waiter_started(participant_id, token, os.getpid())
            next_heartbeat = time.monotonic()
            reported_error = None
            sink: CodexSink | None = None
            conflicted: set[int] = set()
            uncertain: dict[int, float] = {}          # message id -> when to look again
            try:
                if codex_thread is not None:
                    if not codex_thread.strip():
                        raise ValueError("Codex thread ID must not be blank")
                    if shutil.which(CODEX_COMMAND[0]) is None:
                        raise ValueError(f"{CODEX_COMMAND[0]} must be on PATH")
                while not stop.is_set():
                    try:
                        if not store.token_active(token):
                            break
                        for participant_id in store.token_participants(token, without_run=True):
                            store.waiter_started(participant_id, token, os.getpid())
                        if time.monotonic() >= next_heartbeat:
                            store.waiter_heartbeat(token)
                            next_heartbeat = time.monotonic() + 5
                        # An uncertain message is looked for again when its time comes;
                        # until then the ones after it are served.
                        waiting = {id for id, at in uncertain.items() if at > time.monotonic()}
                        message = store.pending_for_token(token, excluding=conflicted | waiting)
                        if message is None:
                            stop.wait(0.5)
                            continue
                        text = render_message(message)
                        if codex_thread is None:
                            print(text, flush=True)
                        else:
                            try:
                                if sink is None:
                                    sink = CodexSink(timeout=CODEX_QUEUE_TIMEOUT_SECONDS, stop=stop)
                                deliver_to_codex(store, sink, codex_thread, message, text)
                            except KeyConflict as conflict:
                                # Permanent: the key is taken by other text. Not acknowledged,
                                # not added, not tried again; said once and left to a person.
                                conflicted.add(message["id"])
                                uncertain.pop(message["id"], None)
                                detail = f"key conflict for message {message['id']}: {conflict}"
                                store.waiter_error(token, detail)
                                print(detail, file=sys.stderr, flush=True)
                                continue
                            except DeliveryUncertain as why:
                                # Handed over once, found nowhere: not added again, not
                                # acknowledged; looked for again later, said once.
                                first = message["id"] not in uncertain
                                uncertain[message["id"]] = time.monotonic() + CODEX_UNCERTAIN_PAUSE_SECONDS
                                store.waiter_error(token, str(why))
                                if first:
                                    print(why, file=sys.stderr, flush=True)
                                continue
                            except SinkUnsupported as unsupported:
                                # No safe way to deliver: nothing is sent, and why is on record.
                                detail = f"codex app-server queue API unsupported: {unsupported}"
                                store.waiter_error(token, detail)
                                if detail != reported_error:
                                    print(detail, file=sys.stderr, flush=True)
                                    reported_error = detail
                                if sink is not None:
                                    sink.close()
                                sink = None
                                stop.wait(CODEX_UNSUPPORTED_PAUSE_SECONDS)
                                continue
                            except (SinkTransportError, SinkError) as error:
                                # The answer is lost or refused; the message stays marked and
                                # unacknowledged, and the next attempt reads before it adds.
                                detail = f"codex app-server {error} for message {message['id']}"
                                store.waiter_error(token, detail)
                                if detail != reported_error:
                                    print(
                                        f"Delivery failed for message {message['id']}; "
                                        f"retrying in {CODEX_RETRY_PAUSE_SECONDS} seconds",
                                        file=sys.stderr,
                                        flush=True,
                                    )
                                    reported_error = detail
                                if sink is not None:
                                    sink.close()
                                sink = None
                                stop.wait(CODEX_RETRY_PAUSE_SECONDS)
                                continue
                        store.ack_for_token(token, message["id"])
                        uncertain.pop(message["id"], None)
                        reported_error = None
                    except sqlite3.OperationalError as error:
                        # Another process holds the database. Keep serving; a message
                        # delivered before a busy ack is repeated and deduplicated by id.
                        # Any other database error ends the waiter with its cause on record.
                        if not is_busy(error):
                            raise
                        detail = f"{type(error).__name__}: {error}"
                        if detail != reported_error:
                            print(f"Database busy ({error}); retrying", file=sys.stderr, flush=True)
                            reported_error = detail
                        stop.wait(0.5)
            except WaiterSignal as stopped:
                name = signal.Signals(stopped.signum).name
                store.waiter_finished(token, "signal", 128 + stopped.signum, name)
                return 128 + stopped.signum
            except Exception as error:
                store.waiter_finished(token, "error", 1, f"{type(error).__name__}: {error}")
                raise
            else:
                why = "stopped by the server" if stop.is_set() else "token inactive"
                try:
                    store.waiter_finished(token, "normal", 0, why)
                except sqlite3.OperationalError as error:
                    # The end is not held up by a database that is busy for it: the
                    # row it could not close lapses with its heartbeat.
                    if not is_busy(error):
                        raise
                    print(f"Waiter ended ({why}) but could not record it: {error}",
                          file=sys.stderr, flush=True)
                return 0
            finally:
                if sink is not None:
                    sink.close()
    finally:
        store.close()


def lock_owned(path: Path, owner: str) -> bool:
    try:
        handle = _open_lock_file(path, os.O_RDWR)
    except OSError:                      # absent, a link, or not a plain file: not ours
        return False
    try:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            handle.seek(0)
            return handle.read().strip() == owner
        return False
    finally:
        handle.close()


def register(args: argparse.Namespace) -> int:
    if args.codex is None:
        raise ValueError("--register requires --codex")
    path = args.db.expanduser().resolve()
    owned = lock_path(path, args.token)
    store = Store(path)
    try:
        def running() -> bool:
            # The lock is taken first and the run row written right after it; join
            # reports active only from the row, so wait for both.
            run = store.last_waiter_for_token(args.token)
            return lock_owned(owned, args.token) and run is not None and run["ended_at"] is None

        if running():
            return 0
        previous = store.last_waiter_for_token(args.token)
        previous_id = previous["id"] if previous is not None else 0
        request_id = store.request_waiter(args.token, args.codex)
        deadline = time.monotonic() + 12
        while time.monotonic() < deadline:
            if running():
                return 0
            latest = store.last_waiter_for_token(args.token)
            if (
                latest is not None
                and latest["id"] > previous_id
                and latest["ended_at"] is not None
            ):
                raise RuntimeError(
                    f"MCP-managed waiter exited: {latest['detail'] or latest['exit_kind']}"
                )
            time.sleep(0.05)
        store.cancel_waiter_request(args.token, request_id)
        raise TimeoutError("MCP server did not start the registered waiter")
    finally:
        store.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("--token", required=True)
    parser.add_argument("--codex", metavar="THREAD")
    parser.add_argument("--register", action="store_true")
    args = parser.parse_args()
    if args.register:
        raise SystemExit(register(args))

    def stop(signum, _frame):
        raise WaiterSignal(signum)

    handled = [signal.SIGINT, signal.SIGTERM]
    if hasattr(signal, "SIGHUP"):
        handled.append(signal.SIGHUP)
    previous = {signum: signal.signal(signum, stop) for signum in handled}
    try:
        raise SystemExit(run(args.db, args.token, args.codex))
    finally:
        for signum, handler in previous.items():
            signal.signal(signum, handler)


if __name__ == "__main__":
    main()
