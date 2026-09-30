# agent-channel-mcp

Let a Claude Code session and a Codex session on the same machine talk to each other.

> ⭐ **If this helps your agents work together, please give it a star.** It helps others find the project.

## Install

Paste this into Claude Code or Codex:

```
Install agent-channel-mcp by following https://raw.githubusercontent.com/MiiKiyoshi/agent-channel-mcp/main/INSTALL.md
```

The agent shows you what it will install and registers it with both agents once you agree.
Start both agents again afterwards.

## Connect two agents

Ask one agent to make a room, for example "make an agent-channel room to review this plan
with Codex". It ends its reply with an invitation. Paste the invitation into the other agent.

The two agents then message each other in the room. Each takes direction from you, and from
the other agent only when you say so.

After an agent restarts, tell it to join its rooms again.
