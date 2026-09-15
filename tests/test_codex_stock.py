"""The delivery against the installed Codex (see stock_codex_scenarios.py). Skipped
where `codex` is not installed or a private network namespace cannot be made."""

import json
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

RUNNER = Path(__file__).with_name("stock_codex_scenarios.py")


def _isolated() -> bool:
    try:
        return subprocess.run(["unshare", "-rn", "true"], capture_output=True, timeout=10).returncode == 0
    except (OSError, subprocess.TimeoutExpired):
        return False


@pytest.fixture(scope="module")
def outcomes(tmp_path_factory) -> dict:
    if shutil.which("codex") is None:
        pytest.skip("codex is not installed")
    if not _isolated():
        pytest.skip("unshare -rn is not available")
    output = tmp_path_factory.mktemp("stock") / "outcomes.json"
    run = subprocess.run(
        ["unshare", "-rn", "sh", "-c", f'ip link set lo up; exec "$0" "$1" "$2"',
         sys.executable, str(RUNNER), str(output)],
        capture_output=True, text=True, timeout=600,
    )
    assert run.returncode == 0, run.stderr[-3000:]
    return json.loads(output.read_text())


def _delivered_once(outcome: dict, text: str | None = None) -> None:
    assert "error" not in outcome, outcome
    assert outcome["items_with_key"] == 1, outcome
    assert outcome["model_inputs_with_text"] == 1, outcome
    assert outcome["queue"] == [], outcome
    if text is not None:
        assert outcome["texts_with_key"] == [text]


def test_a_lost_answer_is_found_at_the_thread_and_not_added_again(outcomes):
    outcome = outcomes["accepted_response_lost"]
    _delivered_once(outcome)
    assert outcome["acknowledged"]


def test_a_message_consumed_before_the_retry_is_found_in_the_items(outcomes):
    outcome = outcomes["consumed_before_retry"]
    _delivered_once(outcome)
    assert outcome["acknowledged"]


def test_a_waiter_that_dies_before_its_ack_is_followed_by_one_that_finds_the_message(outcomes):
    outcome = outcomes["waiter_dies_before_the_ack"]
    _delivered_once(outcome)
    assert outcome["acknowledged"]


def test_two_waiters_retrying_at_once_add_the_message_once(outcomes):
    outcome = outcomes["concurrent_retry"]
    _delivered_once(outcome)
    assert outcome["results"] in (["added", "consumed"], ["added", "queued"], ["added", "uncertain"]), outcome["results"]


def test_a_key_held_by_other_text_is_a_conflict_and_the_message_never_reaches_the_model(outcomes):
    outcome = outcomes["body_conflict"]
    assert "error" not in outcome, outcome
    assert outcome["items_with_key"] == 1
    assert outcome["texts_with_key"] == ["other text under the same key"]
    assert outcome["model_inputs_with_text"] == 0
    assert not outcome["acknowledged"]
    assert "key conflict" in outcome["last_error"]
    assert "consumed with other text" in outcome["conflict"]
