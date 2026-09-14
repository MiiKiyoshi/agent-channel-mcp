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
from agent_channel_mcp.store import PRESENCE_LEASE_SECONDS, Store


def test_identity_takeover_invalidates_old_session(tmp_path):
    path = tmp_path / "db"
    first, second = Channel(path), Channel(path)
    try:
        identity = first.join("room", "claude")["participant"]
        joined = second.join("room", "claude")
        assert joined["participant"] == identity
        assert not first.store.is_active(identity["id"], first.token)
        assert second.store.is_active(identity["id"], second.token)
        with pytest.raises(ValueError, match="no longer owns"):
            first.join("room", "claude")
        with pytest.raises(ValueError, match="no longer owns"):
            first.send("stale", to="claude")
        with pytest.raises(ValueError, match="already joined"):
            first.join("other", "claude")
        first.close()
        first = None
        assert second.store.is_active(identity["id"], second.token)
    finally:
        if first is not None:
            first.close()
        second.close()


def test_reconnect_reports_previous_waiter_without_exit_record(tmp_path):
    path = tmp_path / "db"
    first = Channel(path)
    identity = first.join("room", "plan")["participant"]
    first.store.waiter_started(identity["id"], first.token, 12345)
    first.close()

    second = Channel(path)
    try:
        status = second.join("room", "plan")
        assert status["waiter"] == "offline"
        assert status["waiter_detail"]["reason"] == (
            "not started for this connection; previous waiter process disappeared "
            "without an exit record"
        )
        assert status["waiter_detail"]["previous_connection"] is True
        assert status["waiter_detail"]["pid"] == 12345
    finally:
        second.close()


def test_broadcast_rename_leave_and_rejoin(tmp_path):
    path = tmp_path / "db"
    plan, execute, review = Channel(path), Channel(path), Channel(path)
    try:
        plan.join("room", "plan")
        execute_identity = execute.join("room", "exec")["participant"]
        review.join("room", "review")

        directed = plan.send("one", to="exec")
        assert [delivery["to"] for delivery in directed["deliveries"]] == ["exec"]
        broadcast = plan.send("all")
        assert [delivery["to"] for delivery in broadcast["deliveries"]] == ["exec", "review"]

        renamed = execute.rename("run")
        assert renamed["participant"] == {**execute_identity, "name": "run"}
        assert renamed["registered_roles"] == ["plan", "review", "run"]
        with pytest.raises(ValueError, match="Recipient"):
            plan.send("old name", to="exec")
        assert plan.send("new name", to="run")["deliveries"][0]["to"] == "run"
        with pytest.raises(ValueError, match="already in use"):
            execute.rename("plan")

        old_token = review.token
        assert review.leave()["left"]["name"] == "review"
        assert [p["name"] for p in plan.store.participants("room")] == ["plan", "run"]
        assert plan.store.registered_roles("room") == ["plan", "run"]
        assert all(
            status["name"] != "review"
            for status in plan.store.role_statuses("room")
        )
        assert review.join("other", "review")["participant"]["room"] == "other"
        assert review.token != old_token
    finally:
        plan.close()
        execute.close()
        review.close()


def test_join_returns_waiter_command_and_old_token_is_invalidated(tmp_path):
    first = Channel(tmp_path / "db")
    try:
        result = first.join("room", "claude", "claude-code")
        identity = result["participant"]
        assert result["waiter"] == "offline"
        assert result["waiter_detail"] == {
            "reason": "not started for this connection"
        }
        assert "Monitor(" in result["how"]
        assert Path(shlex.split(result["command"])[1]).exists()
        assert first.join("room", "claude", "claude-code") == result
        first.store.waiter_started(identity["id"], first.token, 12345)
        assert first.join("room", "claude", "claude-code")["waiter"] == "active"

        codex = Channel(tmp_path / "codex-db")
        codex_wait = codex.join("room", "codex", "codex")
        assert "--register --codex" in codex_wait["command"]
        assert "MCP-managed waiter is active" in codex_wait["how"]
        assert 'sandbox_permissions="require_escalated"' in codex_wait["how"]
        assert 'justification="Allow the channel waiter' in codex_wait["how"]
        codex.close()
        other = Channel(tmp_path / "other-db")
        assert "Keep the turn active" in other.join("room", "other")["how"]
        other.close()
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


def test_graceful_close_is_immediately_offline_and_rejoin_reactivates(tmp_path):
    path = tmp_path / "db"
    first = Channel(path)
    identity = first.join("room", "write")["participant"]
    assert first.store.role_statuses("room") == [
        {"name": "write", "connection": "active", "waiter": "offline"}
    ]
    first.close()

    store = Store(path)
    try:
        assert store.registered_roles("room") == ["write"]
        assert store.role_statuses("room") == [
            {"name": "write", "connection": "offline", "waiter": "offline"}
        ]
    finally:
        store.close()

    restarted = Channel(path)
    try:
        joined = restarted.join("room", "write")
        assert joined["participant"] == identity
        assert joined["role_statuses"] == [
            {"name": "write", "connection": "active", "waiter": "offline"}
        ]
    finally:
        restarted.close()


def test_abrupt_client_exit_becomes_offline_after_connection_lease(tmp_path):
    path = tmp_path / "db"
    repository = Path(__file__).resolve().parents[1]
    environment = dict(os.environ)
    environment["PYTHONPATH"] = str(repository)
    script = (
        "import json, sys, time\n"
        "from pathlib import Path\n"
        "from agent_channel_mcp.server import Channel\n"
        "channel = Channel(Path(sys.argv[1]))\n"
        "print(json.dumps(channel.join('room', 'research')), flush=True)\n"
        "time.sleep(60)\n"
    )
    process = subprocess.Popen(
        [sys.executable, "-c", script, str(path)],
        env=environment,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        joined = json.loads(process.stdout.readline())
        process.kill()
        process.wait(timeout=2)

        store = Store(path)
        try:
            heartbeat = store.db.execute(
                "SELECT connection_heartbeat_at FROM participants WHERE id=?",
                (joined["participant"]["id"],),
            ).fetchone()["connection_heartbeat_at"]
            assert store.role_statuses("room", now=heartbeat) == [
                {"name": "research", "connection": "active", "waiter": "offline"}
            ]
            assert store.role_statuses(
                "room", now=heartbeat + PRESENCE_LEASE_SECONDS + 1
            ) == [
                {"name": "research", "connection": "offline", "waiter": "offline"}
            ]
        finally:
            store.close()
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=2)
        if process.stdout:
            process.stdout.close()
        if process.stderr:
            process.stderr.close()


def test_instructions_lead_from_join_to_waiter_command(tmp_path):
    channel = Channel(tmp_path / "db")
    try:
        server = create_server(channel)
        instructions = server.instructions
        descriptions = {tool.name: tool.description for tool in asyncio.run(server.list_tools())}
        join = descriptions["join"]
        assert "only across different session systems" in instructions
        assert "same-system sessions use native communication" in instructions
        assert "Join or create a room" in join
        assert "When asked to create one, choose a descriptive room name" in join
        assert "otherwise ask before creating" in join
        assert "copyable invitation, not a code/link" in join
        assert 'join(room="...", name="<peer role>")' in join
        assert "short, distinct roles suited" in join
        assert "discuss" in join
        assert "waiter=offline, start command using how" in instructions
        assert "registered_roles (not left)" in join
        assert "Do not poll or start duplicate waiters" in instructions
        assert "Do not poll" in instructions
        assert "takes ownership" in join
        assert "omit to to broadcast" in descriptions["send"]
        assert "copyable invitation" not in instructions
        for kept in ("500 UTF-16 code units", "'id sender'", "deduplicate by id",
                     "explicitly delegated authority", "Reply only when needed"):
            assert kept in instructions
        joined = channel.join("room", "claude")
        assert joined["next"].startswith("If waiter is offline, start command using how")
    finally:
        channel.close()


def unpack(result):
    assert not result.isError, result
    return json.loads(result.content[0].text)


def test_two_stdio_clients_and_generated_waiter(tmp_path):
    async def scenario():
        bin_dir = tmp_path / "bin"
        bin_dir.mkdir()
        codex_log = tmp_path / "codex-args.log"
        codex = bin_dir / "codex"
        codex.write_text(
            "#!/bin/sh\nprintf '%s\\0' \"$@\" >> \"$CODEX_ARGS_LOG\"\n",
            encoding="utf-8",
        )
        codex.chmod(0o755)
        environment = dict(os.environ)
        environment["PATH"] = f"{bin_dir}{os.pathsep}{environment['PATH']}"
        environment["CODEX_ARGS_LOG"] = str(codex_log)
        params = StdioServerParameters(
            command=str(Path(sys.executable).with_name("agent-channel-mcp")),
            args=["--db", str(tmp_path / "db")],
            env=environment,
        )
        async with stdio_client(params) as (ar, aw), stdio_client(params) as (br, bw):
            async with ClientSession(ar, aw, client_info=Implementation(name="claude-code", version="test")) as a, \
                    ClientSession(br, bw, client_info=Implementation(name="codex", version="test")) as b:
                assert "waiter=offline, start command using how" in (await a.initialize()).instructions
                await b.initialize()
                assert {tool.name for tool in (await a.list_tools()).tools} == {
                    "join", "send", "rename", "leave",
                }
                assert (await a.call_tool("send", {"to": "codex", "text": "hello"})).isError
                unpack(await a.call_tool("join", {"room": "design-review", "name": "claude"}))
                joined = unpack(await b.call_tool("join", {"room": "design-review", "name": "codex"}))
                assert joined["registered_roles"] == ["claude", "codex"]
                assert [status["connection"] for status in joined["role_statuses"]] == [
                    "active", "active"
                ]
                waiting = joined
                assert waiting["waiter"] == "offline"
                assert "codex queue" in waiting["how"]
                assert 'sandbox_permissions="require_escalated"' in waiting["how"]
                command = shlex.split(waiting["command"])
                command[-1] = "thread-42"
                launcher = subprocess.run(
                    command,
                    env=environment,
                    capture_output=True,
                    text=True,
                    timeout=4,
                )
                assert launcher.returncode == 0, launcher.stderr
                active = unpack(await b.call_tool(
                    "join", {"room": "design-review", "name": "codex"}
                ))
                assert active["waiter"] == "active"

                text = "quotes ' \" $(touch SHOULD_NOT_EXIST) `echo test`\n" + "x" * 501
                sent = unpack(await a.call_tool("send", {"to": "codex", "text": text}))
                message_id = sent["deliveries"][0]["message_id"]
                expected = (f"{message_id} claude\n"
                            "quotes ' \" $(touch SHOULD_NOT_EXIST) `echo test`\n"
                            + "x" * 500 + "\nx")
                for _ in range(100):
                    if codex_log.exists():
                        args = codex_log.read_bytes().split(b"\0")[:-1]
                        if b"--message" in args:
                            delivered = args[args.index(b"--message") + 1].decode()
                            break
                    await asyncio.sleep(0.05)
                else:
                    pytest.fail("managed waiter did not call codex queue")
                assert delivered == expected
                assert not (tmp_path / "SHOULD_NOT_EXIST").exists()
                reply = unpack(await b.call_tool("send", {"to": "claude", "text": "Reviewed"}))
                assert reply["deliveries"][0]["message_id"] > message_id
    asyncio.run(asyncio.wait_for(scenario(), timeout=20))
