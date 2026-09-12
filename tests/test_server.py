import asyncio
import json
import os
import shlex
from pathlib import Path
import subprocess
import sys

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from mcp.types import Implementation
import pytest

from agent_channel_mcp.server import Channel, create_server


def test_identity_lock_and_reconnection(tmp_path):
    path = tmp_path / "db"
    first, second = Channel(path), Channel(path)
    try:
        identity = first.join("room", "claude")["participant"]
        with pytest.raises(ValueError, match="Already running"):
            second.join("room", "claude")
        with pytest.raises(ValueError, match="already joined"):
            first.join("other", "claude")
        first.close()
        first = None
        assert second.join("room", "claude")["participant"] == identity
    finally:
        if first is not None:
            first.close()
        second.close()


def test_wait_requires_join_and_old_token_is_invalidated(tmp_path):
    first = Channel(tmp_path / "db")
    try:
        with pytest.raises(ValueError, match="join"):
            first.wait("codex")
        identity = first.join("room", "claude")["participant"]
        result = first.wait("claude-code")
        assert "Monitor(" in result["how"]
        assert Path(shlex.split(result["command"])[1]).exists()
        assert first.wait("claude-code") == result
        assert "--codex" in first.wait("codex")["command"]
        assert "Keep the turn active" in first.wait("other")["how"]
        token = first.token
    finally:
        first.close()
    second = Channel(tmp_path / "db")
    try:
        second.join("room", "claude")
        assert not second.store.is_active(identity["id"], token)
        assert second.store.is_active(identity["id"], second.token)
    finally:
        second.close()


def test_instructions_lead_from_join_to_wait(tmp_path):
    channel = Channel(tmp_path / "db")
    try:
        instructions = create_server(channel).instructions
        assert "only between different session systems" in instructions
        assert "same-system sessions use native communication" in instructions
        assert "ask before creating one" in instructions
        assert "After approval, create or join it and show" in instructions
        assert "then call wait() and start its returned command exactly once" in instructions
        for kept in ("unique name", "registered name", "500 characters", "'id sender'",
                     "do not add user authorization", "Reply only when needed"):
            assert kept in instructions
        assert channel.join("room", "claude")["next"].startswith("wait() and start its command once;")
    finally:
        channel.close()


def unpack(result):
    assert not result.isError, result
    return json.loads(result.content[0].text)


def test_two_stdio_clients_and_generated_waiter(tmp_path):
    async def scenario():
        params = StdioServerParameters(
            command=sys.executable,
            args=["-m", "agent_channel_mcp.server", "--db", str(tmp_path / "db")],
            env=dict(os.environ),
        )
        async with stdio_client(params) as (ar, aw), stdio_client(params) as (br, bw):
            async with ClientSession(ar, aw, client_info=Implementation(name="claude-code", version="test")) as a, \
                    ClientSession(br, bw, client_info=Implementation(name="codex", version="test")) as b:
                assert "start its returned command exactly once" in (await a.initialize()).instructions
                await b.initialize()
                assert {tool.name for tool in (await a.list_tools()).tools} == {"join", "send", "wait"}
                assert (await a.call_tool("send", {"to": "codex", "text": "hello"})).isError
                unpack(await a.call_tool("join", {"room": "design-review", "name": "claude"}))
                joined = unpack(await b.call_tool("join", {"room": "design-review", "name": "codex"}))
                assert len(joined["participants"]) == 2
                waiting = unpack(await b.call_tool("wait", {}))
                assert "codex queue" in waiting["how"]
                with (tmp_path / "out").open("w") as output:
                    process = subprocess.Popen(shlex.split(waiting["command"])[:2], stdout=output, stderr=subprocess.PIPE,
                                               cwd=tmp_path)
                    try:
                        text = "quotes ' \" $(touch SHOULD_NOT_EXIST) `echo test`\n" + "x" * 501
                        sent = unpack(await a.call_tool("send", {"to": "codex", "text": text}))
                        expected = (f"{sent['message_id']} claude\n"
                                    "quotes ' \" $(touch SHOULD_NOT_EXIST) `echo test`\n" + "x" * 500 + "\nx\n")
                        for _ in range(100):
                            output_text = (tmp_path / "out").read_text()
                            if output_text == expected:
                                break
                            if process.poll() is not None:
                                pytest.fail(process.stderr.read().decode())
                            await asyncio.sleep(0.05)
                        assert output_text == expected
                        assert not (tmp_path / "SHOULD_NOT_EXIST").exists()
                        reply = unpack(await b.call_tool("send", {"to": "claude", "text": "Reviewed"}))
                        assert reply["message_id"] > sent["message_id"]
                    finally:
                        process.terminate()
                        process.wait(timeout=3)
    asyncio.run(asyncio.wait_for(scenario(), timeout=20))
