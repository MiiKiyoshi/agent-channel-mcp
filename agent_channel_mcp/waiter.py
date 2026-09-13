"""Deliver messages as bounded text lines, acknowledging successful delivery."""

import argparse
import fcntl
import os
import signal
import shutil
import subprocess
import sys
import time
from pathlib import Path

from .store import Store


class WaiterSignal(Exception):
    def __init__(self, signum: int):
        self.signum = signum


def lock(path: Path, owner: str | None = None, takeover_timeout: float = 0):
    handle = path.open("a+")
    deadline = time.monotonic() + takeover_timeout
    while True:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
            break
        except BlockingIOError:
            handle.seek(0)
            current_owner = handle.read().strip()
            if owner is None or current_owner == owner or time.monotonic() >= deadline:
                handle.close()
                raise ValueError(f"Already running: {path.name}") from None
            time.sleep(0.05)
    if owner is not None:
        handle.seek(0)
        handle.truncate()
        handle.write(owner)
        handle.flush()
    return handle


def render_message(message: dict) -> str:
    output = []
    text = f"{message['id']} {message['sender']}\n{message['text']}"
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


def run(db: Path, participant_id: str, codex_thread: str | None = None,
        token: str | None = None) -> int:
    store = Store(db)
    try:
        with lock(store.path.with_name(f"{store.path.name}.{participant_id}.wait.lock"),
                  owner=token, takeover_timeout=10 if token is not None else 0):
            run_id = store.waiter_started(participant_id, token, os.getpid())
            next_heartbeat = time.monotonic() + 5
            reported_error = None
            try:
                if codex_thread is not None:
                    if not codex_thread.strip():
                        raise ValueError("Codex thread ID must not be blank")
                    if shutil.which("codex") is None:
                        raise ValueError("codex must be on PATH")
                while token is None or store.is_active(participant_id, token):
                    if time.monotonic() >= next_heartbeat:
                        store.waiter_heartbeat(run_id)
                        next_heartbeat = time.monotonic() + 5
                    message = store.pending(participant_id)
                    if message is None:
                        time.sleep(0.5)
                        continue
                    text = render_message(message)
                    if codex_thread is None:
                        print(text, flush=True)
                    else:
                        result = subprocess.run(
                            ["codex", "queue", "--thread", codex_thread, "--message", text],
                            stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                        )
                        if result.returncode:
                            detail = f"codex queue exited {result.returncode} for message {message['id']}"
                            store.waiter_error(run_id, detail)
                            if detail != reported_error:
                                print(
                                    f"Delivery failed for message {message['id']}; "
                                    "retrying in 5 seconds",
                                    file=sys.stderr,
                                    flush=True,
                                )
                                reported_error = detail
                            time.sleep(5)
                            continue
                    store.ack(participant_id, message["id"])
                    reported_error = None
            except WaiterSignal as stopped:
                name = signal.Signals(stopped.signum).name
                store.waiter_finished(run_id, "signal", 128 + stopped.signum, name)
                return 128 + stopped.signum
            except Exception as error:
                store.waiter_finished(
                    run_id, "error", 1, f"{type(error).__name__}: {error}"
                )
                raise
            else:
                store.waiter_finished(run_id, "normal", 0, "token inactive")
                return 0
    finally:
        store.close()


def _lock_owned(path: Path, owner: str) -> bool:
    try:
        handle = path.open("r+")
    except FileNotFoundError:
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
    if args.codex is None or args.token is None:
        raise ValueError("--register requires --codex and --token")
    path = args.db.expanduser().resolve()
    lock_path = path.with_name(f"{path.name}.{args.participant}.wait.lock")
    if _lock_owned(lock_path, args.token):
        return 0
    store = Store(path)
    try:
        previous = store.last_waiter(args.participant, args.token)
        previous_id = previous["id"] if previous is not None else 0
        store.request_waiter(args.participant, args.token, args.codex)
        deadline = time.monotonic() + 12
        while time.monotonic() < deadline:
            if _lock_owned(lock_path, args.token):
                return 0
            latest = store.last_waiter(args.participant, args.token)
            if (
                latest is not None
                and latest["id"] > previous_id
                and latest["ended_at"] is not None
            ):
                raise RuntimeError(
                    f"MCP-managed waiter exited: {latest['detail'] or latest['exit_kind']}"
                )
            time.sleep(0.05)
        store.cancel_waiter_request(args.participant, args.token)
        raise TimeoutError("MCP server did not start the registered waiter")
    finally:
        store.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("--participant", required=True)
    parser.add_argument("--token")
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
        raise SystemExit(run(args.db, args.participant, args.codex, args.token))
    finally:
        for signum, handler in previous.items():
            signal.signal(signum, handler)


if __name__ == "__main__":
    main()
