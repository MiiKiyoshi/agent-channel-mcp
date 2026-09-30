# Installing agent-channel-mcp

This file is for the agent the user asked to install agent-channel-mcp. Fetch it with
`curl -fsSL`, since a summarizing fetch drops the commands. Inspect first, show one summary,
install after the user agrees. On a machine that already has it, the same steps update it.

## 1. Inspect (change nothing)

- uv: `command -v uv`. Missing: stop and tell the user it is needed
  (https://docs.astral.sh/uv/). Do not install it. uv provides Python 3.10 or newer itself.
- Install directory: `$HOME/.local/opt/agent-channel-mcp`, unless the user named another.
  Note whether it already holds a checkout.
- Agents: `command -v claude` and `command -v codex`. Register with each one found. Two agents
  can talk only when both have it, on this machine and user account.
- Existing registration: `claude mcp get agent-channel`, `codex mcp get agent-channel`. Note a
  directory that differs from the one below.

## 2. Confirm

Show one summary, in the user's language: install directory (new or update), the agents to
register with, and any existing registration that will be replaced. Registration is
user-level, available in every folder. Do not ask about scope. Ask once.

## 3. Install

    DIR="$HOME/.local/opt/agent-channel-mcp"
    git clone https://github.com/MiiKiyoshi/agent-channel-mcp.git "$DIR"   # update: git -C "$DIR" pull --ff-only
    uv sync --directory "$DIR"

## 4. Register

Remove a registration the user agreed to replace (`claude mcp remove --scope user agent-channel`,
`codex mcp remove agent-channel`), then:

    claude mcp add --scope user agent-channel -- uv run --directory "$DIR" --no-sync agent-channel-mcp
    codex mcp add agent-channel -- uv run --directory "$DIR" --no-sync agent-channel-mcp

## 5. Tell the user

The server loads when an agent session starts, so this session cannot use it yet. Start both
agents again. Then ask one of them to make a room, and paste the invitation it ends its reply
with into the other.
