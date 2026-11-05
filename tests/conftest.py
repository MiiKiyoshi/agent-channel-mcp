import pytest

from agent_channel_mcp import plugin_monitor


@pytest.fixture(autouse=True)
def no_plugin_monitor(tmp_path, monkeypatch):
    """No plugin monitor runs for a test's session, whatever runs for the one running the tests."""
    monkeypatch.setattr(plugin_monitor, "DIRECTORY", tmp_path / "no-monitor")
