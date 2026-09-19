"""Contract tests for durable Matrix peer-message buffering."""
from concurrent.futures import ThreadPoolExecutor
import hashlib
import sqlite3
from types import SimpleNamespace

import pytest

from plugins.platforms.matrix.peer_buffer import MatrixPeerBuffer, PeerBufferConfig
from plugins.platforms.matrix import _ack_peer_messages
from hermes_state import SessionDB
from gateway.session_context import clear_session_vars, set_session_vars
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
    assert second.claim_batch(room_id="!r:hs", batch_id=batch["batch_id"], session_key="matrix:!r:hs", turn_key="$turn")
    restarted = MatrixPeerBuffer(db, config)
    recovered = restarted.prepare_batch("!r:hs")
    assert recovered and recovered["batch_id"] == batch["batch_id"]
    assert second.mark_included(room_id="!r:hs", batch_id=batch["batch_id"])
    assert second.complete_batch(room_id="!r:hs", batch_id=batch["batch_id"], success=True) is True
    assert second.acknowledge_batch(room_id="!r:hs", batch_id=batch["batch_id"]) == 2
    assert second.acknowledge_batch(room_id="!r:hs", batch_id=batch["batch_id"]) == 2


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


def test_ack_preserves_omissions_added_after_batch_snapshot(tmp_path):
    buffer = store(tmp_path, max_count=1)
    buffer.add(room_id="!r:hs", event_id="$1", sender="@a:hs", timestamp=1, body="one")
    buffer.add(room_id="!r:hs", event_id="$2", sender="@a:hs", timestamp=2, body="two")
    batch = buffer.prepare_batch("!r:hs")
    assert batch
    assert buffer.claim_batch(room_id="!r:hs", batch_id=batch["batch_id"], session_key="matrix:!r:hs", turn_key="$turn")
    buffer.add(room_id="!r:hs", event_id="$3", sender="@a:hs", timestamp=3, body="three")
    assert buffer.mark_included(room_id="!r:hs", batch_id=batch["batch_id"])
    assert buffer.acknowledge_batch(room_id="!r:hs", batch_id=batch["batch_id"]) == 0
    assert buffer.pending("!r:hs")["omitted_count"] - buffer.pending("!r:hs")["delivered_omitted_count"] == 1


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
    assert buffer.claim_batch(room_id="!r:hs", batch_id=batch["batch_id"], session_key="matrix:!r:hs", turn_key="$turn")
    assert buffer.complete_batch(room_id="!r:hs", batch_id=batch["batch_id"], success=False)
    retry = buffer.prepare_batch("!r:hs")
    assert retry and retry["batch_id"] == batch["batch_id"]
    assert buffer.claim_batch(room_id="!r:hs", batch_id=retry["batch_id"], session_key="matrix:!r:hs", turn_key="$turn2")
    assert buffer.mark_included(room_id="!r:hs", batch_id=batch["batch_id"])
    assert buffer.complete_batch(room_id="!r:hs", batch_id=batch["batch_id"], success=True)
    assert buffer.prepare_batch("!r:hs") is None
    buffer.add(room_id="!r:hs", event_id="$2", sender="@a:hs", timestamp=2, body="two")
    next_batch = buffer.prepare_batch("!r:hs")
    assert next_batch and next_batch["event_ids"] == ["$2"]


def test_ack_tool_rejects_route_key_without_canonical_session(monkeypatch, tmp_path):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / "profile"))
    monkeypatch.setenv("HERMES_SESSION_PLATFORM", "matrix")
    monkeypatch.setenv("HERMES_SESSION_CHAT_ID", "!room:hs")
    buffer = MatrixPeerBuffer(tmp_path / "profile" / "matrix_peer_messages.sqlite3", PeerBufferConfig(enabled=True))
    buffer.add(room_id="!room:hs", event_id="$1", sender="@a:hs", timestamp=1, body="x")
    batch = buffer.prepare_batch("!room:hs")
    assert batch
    assert buffer.claim_batch(room_id="!room:hs", batch_id=batch["batch_id"], session_key="matrix:!room:hs", turn_key="$turn")
    assert buffer.mark_included(room_id="!room:hs", batch_id=batch["batch_id"])
    result = _ack_peer_messages({"batch_id": batch["batch_id"]})
    assert '"ok": false' in result
    assert buffer.pending("!room:hs")["messages"]


def test_ack_tool_uses_canonical_session_and_committed_exact_receipt(monkeypatch, tmp_path):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / "profile"))
    buffer = MatrixPeerBuffer(tmp_path / "profile" / "matrix_peer_messages.sqlite3", PeerBufferConfig(enabled=True))
    buffer.add(room_id="!room:hs", event_id="$peer", sender="@a:hs", timestamp=1, body="x")
    batch = buffer.prepare_batch("!room:hs")
    assert batch
    assert buffer.claim_batch(
        room_id="!room:hs", batch_id=batch["batch_id"],
        session_key="canonical-session", turn_key="$human",
    )
    assert buffer.bind_batch_content(
        room_id="!room:hs", batch_id=batch["batch_id"],
        content_sha256=hashlib.sha256(b"augmented").hexdigest(),
    )
    receipt = {
        "integration": "matrix-peer-buffer", "batch_id": batch["batch_id"],
        "room_id": "!room:hs", "event_ids": ["$peer"],
        "inbound_event_id": "$human",
        "content_sha256": hashlib.sha256(b"augmented").hexdigest(),
    }
    db = SessionDB(tmp_path / "profile" / "state.db")
    db.create_session(session_id="canonical-session", source="matrix", model="test")
    db.append_message(
        session_id="canonical-session", role="user", content="augmented",
        platform_message_id="$human", display_metadata={"integration": receipt},
    )
    db.close()
    tokens = set_session_vars(
        platform="matrix", chat_id="!room:hs", session_key="matrix:route",
        session_id="canonical-session", message_id="$human",
    )
    try:
        result = _ack_peer_messages({"batch_id": batch["batch_id"]})
    finally:
        clear_session_vars(tokens)
    assert '"ok": true' in result
    assert not buffer.pending("!room:hs")["messages"]


def test_real_agent_persists_receipt_before_same_turn_ack_tool(monkeypatch, tmp_path):
    """Real AIAgent loop: pre-LLM DB commit, tool call, then completion."""
    from run_agent import AIAgent
    from plugins.platforms.matrix.tools import _ack_peer_messages as real_ack

    home = tmp_path / "profile"
    monkeypatch.setenv("HERMES_HOME", str(home))
    buffer = MatrixPeerBuffer(home / "matrix_peer_messages.sqlite3", PeerBufferConfig(enabled=True))
    buffer.add(room_id="!room:hs", event_id="$peer", sender="@peer:hs", timestamp=1, body="context")
    batch = buffer.prepare_batch("!room:hs")
    assert batch and buffer.claim_batch(
        room_id="!room:hs", batch_id=batch["batch_id"],
        session_key="canonical-session", turn_key="$human",
    )
    augmented = "peer envelope\n\nhuman turn"
    content_hash = hashlib.sha256(augmented.encode()).hexdigest()
    assert buffer.bind_batch_content(
        room_id="!room:hs", batch_id=batch["batch_id"], content_sha256=content_hash,
    )
    marker = {"integration": {
        "integration": "matrix-peer-buffer", "batch_id": batch["batch_id"],
        "room_id": "!room:hs", "event_ids": ["$peer"],
        "inbound_event_id": "$human", "content_sha256": content_hash,
    }}

    class Completions:
        calls = 0

        def create(self, **_kwargs):
            type(self).calls += 1
            if type(self).calls == 1:
                call = SimpleNamespace(
                    id="ack-1", type="function",
                    function=SimpleNamespace(
                        name="matrix_ack_peer_messages",
                        arguments=f'{{"batch_id":"{batch["batch_id"]}"}}',
                    ),
                )
                message = SimpleNamespace(content=None, reasoning=None, tool_calls=[call])
                finish = "tool_calls"
            else:
                message = SimpleNamespace(content="done", reasoning=None, tool_calls=[])
                finish = "stop"
            return SimpleNamespace(
                choices=[SimpleNamespace(message=message, finish_reason=finish)], usage=None,
            )

    client = SimpleNamespace(chat=SimpleNamespace(completions=Completions()))
    monkeypatch.setattr("agent.process_bootstrap.OpenAI", lambda **_kwargs: client)
    monkeypatch.setattr("model_tools.get_tool_definitions", lambda *_a, **_k: [{
        "type": "function", "function": {
            "name": "matrix_ack_peer_messages", "description": "ack",
            "parameters": {"type": "object", "properties": {"batch_id": {"type": "string"}}},
        },
    }])
    monkeypatch.setattr(
        "model_tools.handle_function_call",
        lambda name, args, *_pos, **_kwargs: real_ack(args) if name == "matrix_ack_peer_messages" else "{}",
    )
    tokens = set_session_vars(
        platform="matrix", chat_id="!room:hs", session_key="matrix:route",
        session_id="canonical-session", message_id="$human",
    )
    db = SessionDB(home / "state.db")
    try:
        agent = AIAgent(
            model="test-model", api_key="test", base_url="acp://fake",
            provider="openai-compat", platform="matrix", session_id="canonical-session",
            session_db=db, max_iterations=3, quiet_mode=True,
            skip_context_files=True, skip_memory=True, save_trajectories=False,
        )
        agent._title_generated = True
        agent.valid_tool_names = {"matrix_ack_peer_messages"}
        result = agent.run_conversation(
            augmented, conversation_history=[], task_id="canonical-session",
            persist_user_platform_id="$human", persist_user_display_metadata=marker,
        )
    finally:
        clear_session_vars(tokens)
        db.close()
    assert result["final_response"] == "done"
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
    event = adapter.handle_message.await_args.args[0]
    event = await adapter.prepare_inbound_event(event, "canonical-session", "$human")
    delivered = event.text
    assert "event_id=$blocked" in delivered
    assert "peer hello" in delivered
    assert adapter._peer_message_buffer.pending("!room1:example.org")["messages"]


@pytest.mark.asyncio
async def test_gateway_lifecycle_claims_and_settles_batch(tmp_path, monkeypatch):
    """The real adapter gateway task claims only after its processing-start hook."""
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
    from gateway.platforms.base import BasePlatformAdapter
    adapter.handle_message = BasePlatformAdapter.handle_message.__get__(adapter, type(adapter))
    async def handler(_event):
        return None
    adapter.set_message_handler(handler)
    await adapter._on_room_message(_make_event("@hermes:example.org go", sender="@alice:example.org", event_id="$turn"))
    task = next(iter(adapter._session_tasks.values()))
    await task
    # A handler completion without a committed SessionDB receipt must not auto-clear.
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
    batch_event = await adapter.prepare_inbound_event(batch_event, "canonical-session", "$ok")
    await adapter.on_processing_start(batch_event)
    await adapter.on_processing_complete(batch_event, __import__("gateway.platforms.base", fromlist=["ProcessingOutcome"]).ProcessingOutcome.SUCCESS)
    assert adapter._peer_message_buffer.pending("!room1:example.org")["messages"]
    batch_event.raw_message["_persisted_integration_receipt"] = True
    batch_event.raw_message["_inbound_model_turn_succeeded"] = True
    await adapter.on_processing_complete(batch_event, __import__("gateway.platforms.base", fromlist=["ProcessingOutcome"]).ProcessingOutcome.SUCCESS)
    assert not adapter._peer_message_buffer.pending("!room1:example.org")["messages"]


    await adapter._on_room_message(_make_event("peer retry", sender="@agent-b:example.org", event_id="$blocked2"))
    adapter.handle_message.side_effect = RuntimeError("turn failed")
    with pytest.raises(RuntimeError):
        await adapter._on_room_message(_make_event("@hermes:example.org retry", sender="@alice:example.org", event_id="$fail"))
    assert adapter._peer_message_buffer.pending("!room1:example.org")["messages"]


@pytest.mark.asyncio
async def test_durable_receipt_does_not_auto_clear_failed_model_turn(tmp_path):
    adapter = _make_adapter(extra={"peer_message_buffer": {"enabled": True, "auto_clear": True}})
    adapter._peer_message_buffer = MatrixPeerBuffer(
        tmp_path / "state.sqlite3", PeerBufferConfig(enabled=True, auto_clear=True))
    store = adapter._peer_message_buffer
    store.add(room_id="!room1:example.org", event_id="$peer", sender="@peer:hs", timestamp=1, body="x")
    batch = store.prepare_batch("!room1:example.org")
    assert batch and store.claim_batch(
        room_id="!room1:example.org", batch_id=batch["batch_id"],
        session_key="canonical", turn_key="$human",
    )
    assert store.bind_batch_content(
        room_id="!room1:example.org", batch_id=batch["batch_id"], content_sha256="1" * 64,
    )
    event = SimpleNamespace(
        message_id="$human", source=SimpleNamespace(chat_id="!room1:example.org"),
        raw_message={
            "_matrix_peer_batch_id": batch["batch_id"],
            "_persisted_integration_receipt": True,
            "_inbound_model_turn_succeeded": False,
        },
    )
    outcome = __import__("gateway.platforms.base", fromlist=["ProcessingOutcome"]).ProcessingOutcome.SUCCESS
    await adapter.on_processing_complete(event, outcome)
    assert store.pending("!room1:example.org")["messages"]


@pytest.mark.asyncio
async def test_committed_sessiondb_user_row_proves_matrix_receipt_after_reopen(tmp_path, monkeypatch):
    monkeypatch.setenv("MATRIX_ALLOWED_USERS", "@alice:example.org,@agent-b:example.org")
    monkeypatch.setenv("MATRIX_REQUIRE_MENTION", "true")
    monkeypatch.setenv("MATRIX_AUTO_THREAD", "false")
    adapter = _make_adapter(extra={"peer_agent_ids": ["@agent-b:example.org"]})
    adapter._peer_message_buffer = MatrixPeerBuffer(
        tmp_path / "profile" / "state.sqlite3", PeerBufferConfig(enabled=True))
    adapter._peer_message_buffer.add(
        room_id="!room1:example.org", event_id="$blocked", sender="@agent-b:example.org",
        timestamp=1, body="peer data",
    )
    await adapter._on_room_message(_make_event("@hermes:example.org go", sender="@alice:example.org", event_id="$turn"))
    event = await adapter.prepare_inbound_event(
        adapter.handle_message.await_args.args[0], "canonical-session", "$turn",
    )
    marker = event.raw_message["_persist_user_display_metadata"]
    db = SessionDB(tmp_path / "profile" / "state.db")
    try:
        db.create_session(session_id="canonical-session", source="matrix", model="test")
        db.append_message(
            session_id="canonical-session", role="user", content=event.text,
            display_metadata=marker, platform_message_id="$turn",
        )
        db.close()
        reopened = SessionDB(tmp_path / "profile" / "state.db")
        try:
            rows = reopened.get_messages_as_conversation("canonical-session")
            integration = marker["integration"]
            committed = reopened.has_inbound_integration_receipt(
                "canonical-session", platform_message_id="$turn",
                integration="matrix-peer-buffer",
                receipt_id=integration["batch_id"],
                content_sha256=integration["content_sha256"],
            )
            assert adapter.inbound_persistence_confirmed(event, committed)
            assert rows[-1]["display_metadata"]["integration"]["batch_id"] == marker["integration"]["batch_id"]
            assert not reopened.has_inbound_integration_receipt(
                "canonical-session", platform_message_id="$wrong",
                integration="matrix-peer-buffer",
                receipt_id=integration["batch_id"],
                content_sha256=integration["content_sha256"],
            )
            assert not reopened.has_inbound_integration_receipt(
                "canonical-session", platform_message_id="$turn",
                integration="matrix-peer-buffer",
                receipt_id=integration["batch_id"], content_sha256="0" * 64,
            )
        finally:
            reopened.close()
    finally:
        try:
            db.close()
        except Exception:
            pass


@pytest.mark.asyncio
async def test_dead_process_claim_without_receipt_is_reclaimed(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path / "profile"))
    monkeypatch.setenv("MATRIX_ALLOWED_USERS", "@alice:example.org,@agent-b:example.org")
    monkeypatch.setenv("MATRIX_REQUIRE_MENTION", "false")
    monkeypatch.setenv("MATRIX_AUTO_THREAD", "false")
    adapter = _make_adapter(extra={"peer_agent_ids": ["@agent-b:example.org"]})
    adapter._peer_message_buffer = MatrixPeerBuffer(
        tmp_path / "profile" / "matrix_peer_messages.sqlite3", PeerBufferConfig(enabled=True))
    store = adapter._peer_message_buffer
    store.add(room_id="!room1:example.org", event_id="$peer", sender="@peer:hs", timestamp=1, body="x")
    batch = store.prepare_batch("!room1:example.org")
    assert batch and store.claim_batch(
        room_id="!room1:example.org", batch_id=batch["batch_id"],
        session_key="old-session", turn_key="$old",
    )
    assert store.bind_batch_content(
        room_id="!room1:example.org", batch_id=batch["batch_id"], content_sha256="1" * 64,
    )
    with sqlite3.connect(store.db_path) as db:
        db.execute("UPDATE peer_batches SET claim_owner='dead-process' WHERE batch_id=?", (batch["batch_id"],))
    await adapter._on_room_message(_make_event("continue", sender="@alice:example.org", event_id="$new"))
    event = adapter.handle_message.await_args.args[0]
    prepared = await adapter.prepare_inbound_event(event, "new-session", "$new")
    assert prepared.raw_message["_matrix_peer_batch_id"] == batch["batch_id"]
    assert "event_id=$peer" in prepared.text


@pytest.mark.asyncio
async def test_command_is_classified_before_peer_context_and_does_not_claim(tmp_path, monkeypatch):
    monkeypatch.setenv("MATRIX_ALLOWED_USERS", "@alice:example.org,@agent-b:example.org")
    monkeypatch.setenv("MATRIX_REQUIRE_MENTION", "true")
    monkeypatch.setenv("MATRIX_AUTO_THREAD", "false")
    adapter = _make_adapter(extra={"peer_agent_ids": ["@agent-b:example.org"]})
    adapter._peer_message_buffer = MatrixPeerBuffer(
        tmp_path / "profile" / "state.sqlite3", PeerBufferConfig(enabled=True))
    adapter._peer_message_buffer.add(
        room_id="!room1:example.org", event_id="$blocked", sender="@agent-b:example.org",
        timestamp=1, body="peer data",
    )
    await adapter._on_room_message(_make_event("/stop", sender="@alice:example.org", event_id="$stop"))
    event = adapter.handle_message.await_args.args[0]
    assert event.message_type.value == "command"
    assert event.text == "/stop"
    assert adapter._peer_message_buffer.prepare_batch("!room1:example.org")

    await adapter._on_room_message(_make_event("peer retry", sender="@agent-b:example.org", event_id="$blocked2"))
    adapter.handle_message.side_effect = RuntimeError("turn failed")
    with pytest.raises(RuntimeError):
        await adapter._on_room_message(_make_event("@hermes:example.org retry", sender="@alice:example.org", event_id="$fail"))
    assert adapter._peer_message_buffer.pending("!room1:example.org")["messages"]
