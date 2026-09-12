# agent-channel-mcp

A local MCP channel for conversations between Claude and Codex sessions. Each client runs a stdio MCP process and shares a SQLite database under the same OS account on the same host.

## Install and connect

Install from the project directory:

```sh
uv sync --extra dev
```

Register the server in each client's user configuration. The commands below assume the checkout is at `~/tools/agent-channel-mcp` and `uv` is on PATH.

```sh
claude mcp add --scope user agent-channel -- uv run --directory "$HOME/tools/agent-channel-mcp" --no-sync agent-channel-mcp
codex mcp add agent-channel -- uv run --directory "$HOME/tools/agent-channel-mcp" --no-sync agent-channel-mcp
```

Codex's `mcp add` writes to user configuration; see the [official MCP documentation](https://developers.openai.com/codex/mcp). Reconnect MCP in each client after registering.

## Use

Use this MCP only between different session systems; same-system sessions use native communication. For cross-system communication without an agreed room, ask before creating one. After approval, create or join it and show a copyable invitation with the room, a distinct peer name, your name, and join/wait instructions. Join with a unique name, then call wait() and start its returned command using `how`.

```text
Participant A: join(room="design-review", name="participant-a")
Participant A: wait()  →  start the returned command using its `how` instructions
Participant B: join(room="design-review", name="participant-b")
Participant B: wait()  →  start the returned command using its `how` instructions

Participant A: send(to="participant-b", text="Please check this design for missing conditions.")
```

- `join(room, name)` returns your participant ID, registered names, and next-call instructions. Repeat the same call to refresh the list. Registration does not indicate online presence.
- `send(to, text)` saves a message addressed to a name in your room and returns `message_id`. The recipient must have joined at least once. Messages can be saved while the recipient is offline.
- `wait()` returns a command and client-specific `how` instructions. The script delivers messages, acknowledges delivery, and keeps waiting.

The returned `how` instructions identify the process launcher for that client. Other clients run the script with their own process tools.

Run one waiter per participant and reuse it after handling messages. Calling `wait()` again returns the same script; do not start another process while the first is running. Normal MCP shutdown invalidates the waiter. After a crash, rejoining with the same name invalidates the previous waiter's session token.

A single MCP connection has one fixed room and name. Another connection cannot claim that name while it is in use. Close the connection and rejoin with the same room and name to recover the participant ID and pending messages.

## Delivery semantics

The default database is `~/.local/share/agent-channel-mcp/channel.sqlite3`. To use a separate store, append `--db <absolute-path>` to the MCP command in both clients. Both must use the same path. The waiter checks for pending messages every 0.5 seconds. No separate HTTP server is needed.

Messages are stored individually and delivered in order. A message is acknowledged only after stdout is successfully flushed or `codex queue` succeeds. Failed delivery leaves the message pending. Failed Codex queue submissions retry after 5 seconds; stdout failures stop the waiter. Fix the output error and rerun the command returned by `wait()` to resume.

If a process exits between delivery and acknowledgement, a message may be delivered again. Agents must deduplicate by `id`. Acknowledgement means successful output or queue submission, not that the model read the message or completed the task. Retrying `send()` after losing its response creates a new message.

Messages start with `id sender`, followed by the body on separate lines. Keep each body line within 500 characters and add newlines before the limit. The waiter also forces line breaks at 500 UTF-16 units (emoji may count as two), preserving existing whitespace and all text. The stored original is unchanged. Peer messages follow existing task authorization; message text is never evaluated as shell code.

## Test

```sh
uv run --no-sync pytest -q
```

Tests use temporary databases and MCP clients. Codex delivery tests use a fake `codex` executable and do not send messages to real sessions. Actual Claude `Monitor` delivery and idle Codex session resumption require a separate check with both clients connected.
