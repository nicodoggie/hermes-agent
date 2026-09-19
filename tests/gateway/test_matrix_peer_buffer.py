"""Contract tests for durable Matrix peer-message buffering."""
from concurrent.futures import ThreadPoolExecutor

import pytest

from plugins.platforms.matrix.peer_buffer import MatrixPeerBuffer, PeerBufferConfig
from plugins.platforms.matrix import _ack_peer_messages
from tests.gateway.test_matrix_mention import _make_adapter, _make_event


def store(tmp_path, **kwargs):
    return MatrixPeerBuffer(tmp_path / "profile" / "state.sqlite3", PeerBufferConfig(enabled=True, **kwargs))


def test_restart_retains_oldest_first_and_ack_is_idempotent(tmp_path):
    db = tmp_path / "profile" / "state.sqlite3"
    config = PeerBufferConfig(enabled=True)
    first = MatrixPeerBuffer(db, config)
    first.add(room_id="!r:hs", event_id="$1", sender="@a:hs", timestamp=1, body="one")
    first.add(room_id="!r:hs", event_id="$2", sender="@a:hs", timestamp=2, body="two")

    second = MatrixPeerBuffer(db, config)
    assert [row["event_id"] for row in second.pending("!r:hs")["messages"]] == ["$1", "$2"]
    batch = second.prepare_batch("!r:hs")
    assert batch and batch["event_ids"] == ["$1", "$2"]
    assert second.complete_batch(room_id="!r:hs", batch_id=batch["batch_id"], success=True) is True
    assert second.acknowledge_batch(room_id="!r:hs", batch_id=batch["batch_id"]) == 2
    with pytest.raises(ValueError):
        second.acknowledge_batch(room_id="!r:hs", batch_id=batch["batch_id"])


def test_concurrent_arrivals_are_all_serialized(tmp_path):
    buffer = store(tmp_path, max_count=20)
    with ThreadPoolExecutor(max_workers=4) as pool:
        list(pool.map(
            lambda index: buffer.add(room_id="!r:hs", event_id=f"${index}", sender="@a:hs",
                                     timestamp=index, body=str(index)), range(10)))
    assert len(buffer.pending("!r:hs")["messages"]) == 10


def test_retention_reports_evictions_and_truncates_at_utf8_boundary(tmp_path):
    buffer = store(tmp_path, max_count=2, max_message_bytes=5, max_bytes=100)
    buffer.add(room_id="!r:hs", event_id="$1", sender="@a:hs", timestamp=1, body="😀😀😀")
    buffer.add(room_id="!r:hs", event_id="$2", sender="@a:hs", timestamp=2, body="two")
    buffer.add(room_id="!r:hs", event_id="$3", sender="@a:hs", timestamp=3, body="three")
    pending = buffer.pending("!r:hs")
    assert [row["event_id"] for row in pending["messages"]] == ["$2", "$3"]
    assert pending["omitted_count"] == 1
    assert all(len(row["body"].encode("utf-8")) <= 5 for row in pending["messages"])
    assert "truncated" in pending["messages"][-1]["body"] or pending["messages"][-1]["body"] == "three"


def test_ack_cannot_clear_another_room(tmp_path):
    buffer = store(tmp_path)
    buffer.add(room_id="!other:hs", event_id="$secret", sender="@a:hs", timestamp=1, body="x")
    with pytest.raises(ValueError):
        buffer.acknowledge(room_id="!current:hs", event_ids=["$secret"])
    assert buffer.pending("!other:hs")["messages"]


def test_successful_non_auto_clear_batch_is_not_reappended_but_failure_retries(tmp_path):
    buffer = store(tmp_path)
    buffer.add(room_id="!r:hs", event_id="$1", sender="@a:hs", timestamp=1, body="one")
    batch = buffer.prepare_batch("!r:hs")
    assert batch
    assert buffer.complete_batch(room_id="!r:hs", batch_id=batch["batch_id"], success=False)
    retry = buffer.prepare_batch("!r:hs")
    assert retry and retry["batch_id"] == batch["batch_id"]
    assert buffer.complete_batch(room_id="!r:hs", batch_id=batch["batch_id"], success=True)
    assert buffer.prepare_batch("!r:hs") is None
    buffer.add(room_id="!r:hs", event_id="$2", sender="@a:hs", timestamp=2, body="two")
    next_batch = buffer.prepare_batch("!r:hs")
    assert next_batch and next_batch["event_ids"] == ["$2"]


def test_ack_tool_is_current_matrix_room_scoped(monkeypatch, tmp_path):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / "profile"))
    monkeypatch.setenv("HERMES_SESSION_PLATFORM", "matrix")
    monkeypatch.setenv("HERMES_SESSION_CHAT_ID", "!room:hs")
    buffer = MatrixPeerBuffer(tmp_path / "profile" / "matrix_peer_messages.sqlite3", PeerBufferConfig(enabled=True))
    buffer.add(room_id="!room:hs", event_id="$1", sender="@a:hs", timestamp=1, body="x")
    batch = buffer.prepare_batch("!room:hs")
    assert batch
    result = _ack_peer_messages({"batch_id": batch["batch_id"]})
    assert '"ok": true' in result
    assert not buffer.pending("!room:hs")["messages"]


@pytest.mark.asyncio
async def test_gated_peer_is_buffered_without_model_call_then_delivered(monkeypatch, tmp_path):
    monkeypatch.setenv("MATRIX_ALLOWED_USERS", "@alice:example.org,@agent-b:example.org")
    monkeypatch.setenv("MATRIX_REQUIRE_MENTION", "true")
    monkeypatch.setenv("MATRIX_AUTO_THREAD", "false")
    adapter = _make_adapter(extra={
        "peer_agent_ids": ["@agent-b:example.org"],
        "peer_reply_budget_per_human_message": 1,
        "peer_message_buffer": {"enabled": True},
    })
    adapter._peer_message_buffer = MatrixPeerBuffer(
        tmp_path / "profile" / "state.sqlite3", PeerBufferConfig(enabled=True))
    await adapter._on_room_message(_make_event(
        "peer hello", sender="@agent-b:example.org", event_id="$blocked"))
    adapter.handle_message.assert_not_awaited()

    await adapter._on_room_message(_make_event(
        "@hermes:example.org continue", sender="@alice:example.org", event_id="$human"))
    adapter.handle_message.assert_awaited_once()
    delivered = adapter.handle_message.await_args.args[0].text
    assert "event_id=$blocked" in delivered
    assert "peer hello" in delivered
    assert adapter._peer_message_buffer.pending("!room1:example.org")["messages"]


@pytest.mark.asyncio
async def test_auto_clear_happens_only_after_successful_turn(monkeypatch, tmp_path):
    monkeypatch.setenv("MATRIX_ALLOWED_USERS", "@alice:example.org,@agent-b:example.org")
    monkeypatch.setenv("MATRIX_REQUIRE_MENTION", "true")
    monkeypatch.setenv("MATRIX_AUTO_THREAD", "false")
    adapter = _make_adapter(extra={
        "peer_agent_ids": ["@agent-b:example.org"],
        "peer_message_buffer": {"enabled": True, "auto_clear": True},
    })
    adapter._peer_message_buffer = MatrixPeerBuffer(
        tmp_path / "profile" / "state.sqlite3", PeerBufferConfig(enabled=True, auto_clear=True))
    await adapter._on_room_message(_make_event("peer hello", sender="@agent-b:example.org", event_id="$blocked"))
    await adapter._on_room_message(_make_event("@hermes:example.org go", sender="@alice:example.org", event_id="$ok"))
    batch_event = adapter.handle_message.await_args.args[0]
    await adapter.on_processing_complete(batch_event, __import__("gateway.platforms.base", fromlist=["ProcessingOutcome"]).ProcessingOutcome.SUCCESS)
    assert not adapter._peer_message_buffer.pending("!room1:example.org")["messages"]

    await adapter._on_room_message(_make_event("peer retry", sender="@agent-b:example.org", event_id="$blocked2"))
    adapter.handle_message.side_effect = RuntimeError("turn failed")
    with pytest.raises(RuntimeError):
        await adapter._on_room_message(_make_event("@hermes:example.org retry", sender="@alice:example.org", event_id="$fail"))
    assert adapter._peer_message_buffer.pending("!room1:example.org")["messages"]
