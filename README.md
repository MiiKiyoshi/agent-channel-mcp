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

Ask your agent to create a room and write an invitation. It chooses a descriptive room name and joins; `join` creates the room if needed. Roles should be short, distinct, and fit the task, such as `plan`, `exec`, or `discuss`. The invitation is text to paste into the other harness. It holds the role's purpose in a line, the `join` call, and the waiter step, and nothing else: the task's detail reaches the new agent from `plan` inside the room, not from the invitation.

```text
Purpose: <the role's purpose, in a line>.
Call agent-channel join(room="<room>", name="<peer role>").
Follow the returned how if waiter is offline; if active, do nothing.
```

## Join, wait, and send

On a new MCP connection, the agent calls `join(room, name)`. The response separates `registered_roles`, which lists registration records that have not called `leave()`, from `role_statuses`, whose connection and waiter heartbeat leases are `active` or `offline`. An offline role can still receive queued messages. If `join` returns `waiter: offline`, the agent launches the returned `command` using `how`; if it returns `active`, it does nothing. It keeps that waiter running and does not poll. The Codex command registers a waiter managed by the MCP server, while Claude Code keeps the waiter in a persistent Monitor.

One connection can join several rooms by calling `join` again with another room; `rooms` in the response lists them. A room can carry standing rules: `join(room, name, policy="...")` stores them for the room, every `join` returns them as `policy`, and an empty policy clears them. The existing waiter delivers every room, so a second `join` reports it `active` and returns no command. Room and role names contain no whitespace and are at most 200 UTF-16 code units.

Ask the agent to send directly with `send(text="...", to="exec")`; omitting `to` broadcasts to every other role in that room. It uses `rename(name="...")` if its role changes and `leave()` when leaving. With several rooms joined, `send`, `rename`, and `leave` take `room="..."`; with one room it may be omitted. Leaving the last room stops the waiter. Offline recipients remain queued, but delivery can repeat after an interrupted acknowledgement, so agents deduplicate by message `id`.

A newly joined agent sends to the room's `plan` first, when the room has one. `plan` answers with the current state, the settled contracts, the assets in hand, and the tasks that fall to the new role. Deliveries begin with `id room sender`. Keep each body line within 500 UTF-16 code units; the waiter also wraps longer lines without dropping text. A peer directs work only when the user explicitly delegated authority to that role; the briefing from `plan` is under the same rule.

## Restart or reconnect

After either harness, client, or server restarts, tell the agent to join each of its rooms again and follow the live-status procedure above; the previous connection's room list is not restored automatically, and never assume its waiter survived. Joining the same room and role reuses the registration and recovers pending messages. A graceful exit becomes offline immediately; an interrupted process becomes offline when its short heartbeat lease expires. `leave()` removes the registration immediately but is not required for accurate live status.

## Troubleshooting

- **Recipient not found:** ask that agent to join the room, then tell the sending agent to call `join` again to refresh registered roles and live status.
- **Messages do not arrive:** confirm both MCP registrations run under the same OS account. If either uses `--db`, both must use the same absolute path.
- **Duplicate delivery:** tell the receiving agent to process each message `id` once. A Codex thread receives each message once: the waiter queues it under the key `agent-channel:<channel id>:<message id>` through a stock `codex app-server` child, and before any retry it reads the thread's queue and items for that key.
- **Codex message not delivered, `waiter_detail` says "key conflict":** the message's key is held at the thread by a message with other text. It is not queued and not acknowledged; a person decides.
- **`waiter_detail` says "queue API unsupported":** the installed `codex app-server` does not offer the thread queue API. Nothing is delivered to Codex until a Codex with that API is installed.
- **Waiter will not start:** tell the agent to call `join`, start its command only for `offline`, and leave `active` alone.
- **Waiter disappeared:** inspect `waiter_detail` from `join`. It records normal token shutdowns, signals, runtime errors, last heartbeat, and any failed delivery attempt. An abrupt kill becomes offline when its heartbeat lease expires.

## Test and contribute

Install development dependencies and run the full test suite before submitting a change:

```sh
uv sync --extra dev
uv run --no-sync pytest -q
```

Tests use temporary databases and a fake `codex app-server`; the installed Codex is exercised in a private network namespace where `codex` and `unshare` are available. Live delivery checks require both real harnesses to be connected.
