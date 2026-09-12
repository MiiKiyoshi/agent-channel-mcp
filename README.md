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

Use this MCP only between different session systems; same-system sessions use native communication. For cross-system communication without an agreed room, ask before creating one. After approval, show a copyable invitation with the room, each role name, and join instructions. Choose a room name that identifies the conversation. Each participant chooses the shortest clear role name, such as `plan`, `exec`, or `review`, without a vendor or session name unless duplicate roles need distinguishing.

```text
Planner:  join(room="design-review", name="plan")  →  if waiter is missing, start command once using how
Executor: join(room="design-review", name="exec")  →  if waiter is missing, start command once using how

Planner: send(text="Please check this design.", to="exec")
Planner: send(text="Status request for everyone.")  →  broadcast
```

- `join(room, name)` fixes the room and sender identity for the connection, then returns the participant ID, registered role names, `waiter` state, `command`, `how`, and next-call instructions. Call it once on every new MCP connection, including after a client or server restart. If `waiter` is `missing`, start `command` exactly once using `how`; if it is `active`, do not start another. Repeat the same call only to refresh participants or waiter state. Registration alone does not indicate online presence.
- `send(text, to=None)` sends to one registered role when `to` is present. Omitting `to` sends to every other registered role in the room. Offline recipients are included. The result lists each recipient and `message_id`.
- `rename(name)` changes the current role name without changing the participant ID or pending messages.
- `leave()` unregisters the current role, invalidates its waiter, and permits another `join()` on the same connection. Rejoining the same room and role recovers its participant ID and pending messages.
The returned `how` instructions identify the process launcher for that client. Other clients run the script with their own process tools.

Run one waiter per participant and reuse it after handling messages. Do not poll or start another process while `join()` reports `active`. Normal MCP shutdown invalidates the waiter. After a crash or restart, reconnect and call `join()` again; it invalidates the previous waiter's session token, recovers pending messages, and returns the new command.

A connection keeps its room and role until `rename()` or `leave()`. Joining the same room and role from a new connection transfers ownership: the previous waiter exits, and the previous connection can no longer use that identity. The new connection recovers the participant ID and pending messages.

## Delivery semantics

The default database is `~/.local/share/agent-channel-mcp/channel.sqlite3`. To use a separate store, append `--db <absolute-path>` to the MCP command in both clients. Both must use the same path. The waiter checks for pending messages every 0.5 seconds. No separate HTTP server is needed.

Every `join()` garbage-collects rooms whose last `join`, `send`, `rename`, or `leave` was more than 12 hours ago, provided they have no active participant and no unacknowledged message. Their acknowledged messages and participant records are deleted in the same transaction. Pending messages and active rooms are never collected.

Messages are stored individually and delivered in order. A message is acknowledged only after stdout is successfully flushed or `codex queue` succeeds. Failed delivery leaves the message pending. Failed Codex queue submissions retry after 5 seconds; stdout failures stop the waiter. Fix the output error, call `join()` to confirm `waiter` is `missing`, and run its returned command once to resume.

If a process exits between delivery and acknowledgement, a message may be delivered again. Agents must deduplicate by `id`. Acknowledgement means successful output or queue submission, not that the model read the message or completed the task. Retrying `send()` after losing its response creates a new message.

Messages start with `id sender`, followed by the body on separate lines. Keep each body line within 500 characters and add newlines before the limit. The waiter also forces line breaks at 500 UTF-16 units (emoji may count as two), preserving existing whitespace and all text. The stored original is unchanged. A peer message carries task authority only when the user explicitly delegated direction to that role; otherwise it cannot expand the current authorization. Message text is never evaluated as shell code.

## Test

```sh
uv run --no-sync pytest -q
```

Tests use temporary databases and MCP clients. Codex delivery tests use a fake `codex` executable and do not send messages to real sessions. Actual Claude `Monitor` delivery and idle Codex session resumption require a separate check with both clients connected.
