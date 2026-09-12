"""Deliver messages as bounded text lines, acknowledging successful delivery."""

import argparse
import fcntl
import shutil
import subprocess
import sys
import time
from pathlib import Path

from .store import Store


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
        token: str | None = None) -> None:
    if codex_thread is not None:
        if not codex_thread.strip():
            raise ValueError("Codex thread ID must not be blank")
        if shutil.which("codex") is None:
            raise ValueError("codex must be on PATH")
    store = Store(db)
    try:
        with lock(store.path.with_name(f"{store.path.name}.{participant_id}.wait.lock"),
                  owner=token, takeover_timeout=10 if token is not None else 0):
            while token is None or store.is_active(participant_id, token):
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
                        print(f"Delivery failed for message {message['id']}; retrying in 5 seconds",
                              file=sys.stderr, flush=True)
                        time.sleep(5)
                        continue
                store.ack(participant_id, message["id"])
    finally:
        store.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("--participant", required=True)
    parser.add_argument("--token")
    parser.add_argument("--codex", metavar="THREAD")
    args = parser.parse_args()
    run(args.db, args.participant, args.codex, args.token)


if __name__ == "__main__":
    main()
