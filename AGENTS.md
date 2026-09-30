Work directly on main; do not create task branches; after delivery, local and origin expose only main.

Tests: `uv sync --extra dev`, then `uv run --no-sync pytest -q`. They use temporary databases and a fake `codex app-server`. The installed Codex is exercised in a private network namespace where `codex` and `unshare` are available.
