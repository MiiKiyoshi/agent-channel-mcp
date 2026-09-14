# agent-channel-mcp

## What it does

Connects local agent sessions running in different harnesses, such as Claude Code and Codex. Sessions in the same harness should use its native communication instead. Both harnesses run this MCP server on the same host and OS account.

## Install and connect

Requires Python 3.10 or newer, [`uv`](https://docs.astral.sh/uv/), and a harness with MCP support.

```sh
git clone https://github.com/MiiKiyoshi/agent-channel-mcp.git
cd agent-channel-mcp
uv sync
claude mcp add --scope user agent-channel -- uv run --directory "$PWD" --no-sync agent-channel-mcp
codex mcp add agent-channel -- uv run --directory "$PWD" --no-sync agent-channel-mcp
```

Reconnect MCP in both harnesses after registration. See the [Codex MCP documentation](https://developers.openai.com/codex/mcp) for Codex configuration details.

## Create a room and invite the other agent

Choose a conversation-specific room name and short roles such as `plan`, `exec`, or `review`. Paste this template into each harness with that agent's role:

```text
Join agent-channel room "<room>" as "<role>".
Call join(room="<room>", name="<role>") and follow its waiter instructions.
```

## Join, wait, and send

On a new MCP connection, the agent calls `join(room, name)`. The response separates `registered_roles`, which lists registration records that have not called `leave()`, from `role_statuses`, whose connection and waiter heartbeat leases are `active` or `offline`. An offline role can still receive queued messages. If `join` returns `waiter: offline`, the agent launches the returned `command` using `how`; if it returns `active`, it does nothing. It keeps that waiter running and does not poll. The Codex command registers a waiter managed by the MCP server, while Claude Code keeps the waiter in a persistent Monitor.

Ask the agent to send directly with `send(text="...", to="exec")`; omitting `to` broadcasts to every other role. It uses `rename(name="...")` if its role changes and `leave()` when leaving. Offline recipients remain queued, but delivery can repeat after an interrupted acknowledgement, so agents deduplicate by message `id`.

Deliveries begin with `id sender`. Keep each body line within 500 UTF-16 code units; the waiter also wraps longer lines without dropping text. A peer directs work only when the user explicitly delegated authority to that role.

## Restart or reconnect

After either harness, client, or server restarts, tell the agent to call `join` again and follow the live-status procedure above; never assume the previous connection or waiter survived. Joining the same room and role reuses the registration and recovers pending messages. A graceful exit becomes offline immediately; an interrupted process becomes offline when its short heartbeat lease expires. `leave()` removes the registration immediately but is not required for accurate live status.

## Troubleshooting

- **Recipient not found:** ask that agent to join the room, then tell the sending agent to call `join` again to refresh registered roles and live status.
- **Messages do not arrive:** confirm both MCP registrations run under the same OS account. If either uses `--db`, both must use the same absolute path.
- **Duplicate delivery:** tell the receiving agent to process each message `id` once.
- **Waiter will not start:** tell the agent to call `join`, start its command only for `offline`, and leave `active` alone.
- **Waiter disappeared:** inspect `waiter_detail` from `join`. It records normal token shutdowns, signals, runtime errors, last heartbeat, and any failed `codex queue` attempt. An abrupt kill becomes offline when its heartbeat lease expires.

## Test and contribute

Install development dependencies and run the full test suite before submitting a change:

```sh
uv sync --extra dev
uv run --no-sync pytest -q
```

Tests use temporary databases and a fake Codex executable. Live delivery checks require both real harnesses to be connected.
