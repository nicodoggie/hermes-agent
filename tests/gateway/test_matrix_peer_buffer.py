"""Contract tests for durable Matrix peer-message buffering."""
from concurrent.futures import ThreadPoolExecutor
import hashlib
import sqlite3
from types import SimpleNamespace
from unittest.mock import AsyncMock

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


def test_delivery_then_ack_advances_omission_snapshot_exactly_once(tmp_path):
    buffer = store(tmp_path, max_count=1)
    buffer.add(room_id="!r:hs", event_id="$1", sender="@a:hs", timestamp=1, body="one")
    buffer.add(room_id="!r:hs", event_id="$2", sender="@a:hs", timestamp=2, body="two")
    batch = buffer.prepare_batch("!r:hs")
    assert batch and batch["omitted_count"] == 1
    assert buffer.claim_batch(
        room_id="!r:hs", batch_id=batch["batch_id"], session_key="canonical", turn_key="$turn",
    )
    assert buffer.mark_included(room_id="!r:hs", batch_id=batch["batch_id"])
    assert buffer.complete_batch(room_id="!r:hs", batch_id=batch["batch_id"], success=True)
    buffer.add(room_id="!r:hs", event_id="$3", sender="@a:hs", timestamp=3, body="three")
    assert buffer.acknowledge_batch(
        room_id="!r:hs", batch_id=batch["batch_id"], session_key="canonical",
    ) == 0
    pending = buffer.pending("!r:hs")
    assert pending["omitted_count"] - pending["delivered_omitted_count"] == 1


def test_equal_sized_omission_only_generations_have_distinct_identities(tmp_path):
    buffer = store(tmp_path, max_count=0)
    identities = []
    for generation in range(2):
        buffer.add(
            room_id="!r:hs", event_id=f"${generation}", sender="@a:hs",
            timestamp=generation, body="evicted",
        )
        batch = buffer.prepare_batch("!r:hs")
        assert batch and batch["event_ids"] == [] and batch["omitted_count"] == 1
        identities.append(batch["batch_id"])
        assert buffer.claim_batch(
            room_id="!r:hs", batch_id=batch["batch_id"],
            session_key="canonical", turn_key=f"$turn-{generation}",
        )
        assert buffer.mark_included(room_id="!r:hs", batch_id=batch["batch_id"])
        assert buffer.complete_batch(room_id="!r:hs", batch_id=batch["batch_id"], success=True)
    assert identities[0] != identities[1]


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


def test_registry_discovers_and_dispatches_delivered_batch_read_after_session_transition(monkeypatch, tmp_path):
    """Real plugin discovery/schema/registry dispatch; only the model network is absent."""
    home = tmp_path / "profile"
    monkeypatch.setenv("HERMES_HOME", str(home))
    buffer = MatrixPeerBuffer(home / "matrix_peer_messages.sqlite3", PeerBufferConfig(enabled=True))
    buffer.add(room_id="!room:hs", event_id="$peer", sender="@peer:hs", timestamp=1, body="retained")
    batch = buffer.prepare_batch("!room:hs")
    assert batch and buffer.claim_batch(
        room_id="!room:hs", batch_id=batch["batch_id"], session_key="old-canonical", turn_key="$human",
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
    db = SessionDB(home / "state.db")
    db.create_session(session_id="old-canonical", source="matrix", model="test")
    db.append_message(
        session_id="old-canonical", role="user", content=augmented,
        platform_message_id="$human", display_metadata=marker,
    )
    db.close()
    assert buffer.mark_included(room_id="!room:hs", batch_id=batch["batch_id"])
    assert buffer.complete_batch(room_id="!room:hs", batch_id=batch["batch_id"], success=True)

    import model_tools
    from tools.registry import registry
    from hermes_cli.plugins import discover_plugins
    discover_plugins(force=True)
    model_tools.get_tool_definitions(enabled_toolsets=["hermes-matrix"])
    for name in ("matrix_read_peer_messages", "matrix_ack_peer_messages"):
        entry = registry.get_entry(name)
        assert entry is not None and entry.schema["name"] == name
    tokens = set_session_vars(
        platform="matrix", chat_id="!room:hs", session_key="matrix:route",
        session_id="new-canonical", message_id="$new",
    )
    try:
        listing = model_tools.handle_function_call("matrix_read_peer_messages", {})
        assert batch["batch_id"] in listing
        read = model_tools.handle_function_call(
            "matrix_read_peer_messages", {"batch_id": batch["batch_id"]},
        )
        assert '"body": "retained"' in read
        acknowledged = model_tools.handle_function_call(
            "matrix_ack_peer_messages", {"batch_id": batch["batch_id"]},
        )
    finally:
        clear_session_vars(tokens)
    assert '"ok": true' in acknowledged
    assert not buffer.pending("!room:hs")["messages"]


def test_read_exact_old_batch_and_page_bounded_discovery(monkeypatch, tmp_path):
    home = tmp_path / "profile"
    monkeypatch.setenv("HERMES_HOME", str(home))
    buffer = MatrixPeerBuffer(home / "matrix_peer_messages.sqlite3", PeerBufferConfig(enabled=True))
    db = SessionDB(home / "state.db")
    db.create_session(session_id="old-canonical", source="matrix", model="test")
    batches = []
    for index in range(21):
        event_id = f"$peer-{index}"
        turn_id = f"$human-{index}"
        buffer.add(
            room_id="!room:hs", event_id=event_id, sender="@peer:hs",
            timestamp=index, body=f"retained-{index}",
        )
        batch = buffer.prepare_batch("!room:hs")
        assert batch and buffer.claim_batch(
            room_id="!room:hs", batch_id=batch["batch_id"],
            session_key="old-canonical", turn_key=turn_id,
        )
        content = f"peer envelope {index}"
        content_hash = hashlib.sha256(content.encode()).hexdigest()
        assert buffer.bind_batch_content(
            room_id="!room:hs", batch_id=batch["batch_id"], content_sha256=content_hash,
        )
        db.append_message(
            session_id="old-canonical", role="user", content=content,
            platform_message_id=turn_id, display_metadata={"integration": {
                "integration": "matrix-peer-buffer", "batch_id": batch["batch_id"],
                "inbound_event_id": turn_id, "content_sha256": content_hash,
            }},
        )
        assert buffer.mark_included(room_id="!room:hs", batch_id=batch["batch_id"])
        assert buffer.complete_batch(room_id="!room:hs", batch_id=batch["batch_id"], success=True)
        batches.append(batch["batch_id"])
    db.close()

    import model_tools
    model_tools.get_tool_definitions(enabled_toolsets=["hermes-matrix"])
    tokens = set_session_vars(
        platform="matrix", chat_id="!room:hs", session_key="matrix:route",
        session_id="new-canonical", message_id="$new",
    )
    try:
        first_page = __import__("json").loads(model_tools.handle_function_call(
            "matrix_read_peer_messages", {},
        ))
        assert len(first_page["batches"]) == 20
        assert batches[0] not in {row["batch_id"] for row in first_page["batches"]}
        second_page = __import__("json").loads(model_tools.handle_function_call(
            "matrix_read_peer_messages", {"before_batch_id": first_page["next_before_batch_id"]},
        ))
        assert [row["batch_id"] for row in second_page["batches"]] == [batches[0]]
        exact = model_tools.handle_function_call(
            "matrix_read_peer_messages", {"batch_id": batches[0]},
        )
    finally:
        clear_session_vars(tokens)
    assert '"body": "retained-0"' in exact


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
    delivered = adapter.attach_inbound_context(event, event.text)
    assert "event_id=$blocked" in delivered
    assert "peer hello" in delivered
    assert adapter._peer_message_buffer.pending("!room1:example.org")["messages"]


@pytest.mark.asyncio
async def test_shared_room_prefix_receipt_acks_final_canonical_user_row(monkeypatch, tmp_path):
    """The receipt follows generic inbound transforms through the real registry ack."""
    import model_tools
    import gateway.run_turn as run_turn
    from gateway.config import Platform
    from gateway.run_inbound import GatewayInboundMixin
    from gateway.run_turn import GatewayTurnMixin
    from gateway.session import SessionSource

    home = tmp_path / "profile"
    monkeypatch.setenv("HERMES_HOME", str(home))
    adapter = _make_adapter(extra={"peer_message_buffer": {"enabled": True}})
    adapter._peer_message_buffer = MatrixPeerBuffer(
        home / "matrix_peer_messages.sqlite3", PeerBufferConfig(enabled=True),
    )
    adapter._peer_message_buffer.add(
        room_id="!room:hs", event_id="$peer", sender="@peer:hs", timestamp=1,
        body="literal @file:peer-secret.txt",
    )
    source = SessionSource(
        platform=Platform.MATRIX, chat_id="!room:hs", chat_type="group",
        user_id="@alice:hs", user_name="Alice",
    )
    event = __import__("gateway.platforms.base", fromlist=["MessageEvent"]).MessageEvent(
        text="human turn", source=source, raw_message={}, message_id="$human",
        media_urls=["/tmp/report.pdf"], media_types=["application/pdf"],
        reply_to_message_id="$reply", reply_to_text="literal @file:reply-secret.txt",
    )

    class TestRunner(GatewayTurnMixin, GatewayInboundMixin):
        pass

    runner = TestRunner()
    runner.config = SimpleNamespace(
        group_sessions_per_user=False, thread_sessions_per_user=False, multiplex_profiles=False,
    )
    runner._peek_session_state = lambda _key: None
    runner._adapter_for_source = lambda _source: adapter
    expanded_inputs = []
    async def expand_trigger(_source, _key, text):
        expanded_inputs.append(text)
        return text.replace("@file:human.txt", "[authorized human reference]")
    runner._expand_inbound_context_references = AsyncMock(side_effect=expand_trigger)
    runner.async_session_store = SimpleNamespace(load_transcript=AsyncMock(return_value=[]))
    runner._hmwa_open_session = AsyncMock(return_value=(False, False))
    runner._set_session_env = lambda _context: []
    runner._clear_session_env = lambda _tokens: None
    runner._pinned_session_context_prompt = lambda *_args: "prompt"
    runner._hmwa_acquire_turn_lease = AsyncMock()
    runner._mark_durable_active_turn = AsyncMock()
    runner._hmwa_run_session_hygiene = AsyncMock(return_value=[])
    runner._hmwa_first_contact_notes = AsyncMock()
    runner._voice_channel_sidecar_note = lambda *_args: None
    runner._bind_adapter_run_generation = lambda *_args: None
    monkeypatch.setattr(
        run_turn, "build_session_context",
        lambda _source, _config, entry: SimpleNamespace(session_id=entry.session_id),
    )
    entry = SimpleNamespace(session_id="canonical-session", session_key="matrix:route")
    event.text = "human turn @file:human.txt"
    prepared, _tokens = await runner._hmwa_prepare_turn(
        event, source, entry, "matrix:route", "quick", 1,
    )
    canonical_content = (
        prepared.persist_user_message
        if prepared.persist_user_message is not None else prepared.message_text
    )
    marker = event.raw_message["_persist_user_display_metadata"]

    db = SessionDB(home / "state.db")
    db.create_session(session_id="canonical-session", source="matrix", model="test")
    db.append_message(
        session_id="canonical-session", role="user", content=canonical_content,
        platform_message_id="$human", display_metadata=marker,
    )
    rows = db.get_messages_as_conversation("canonical-session")
    integration = marker["integration"]
    assert integration["content_sha256"] == hashlib.sha256(canonical_content.encode()).hexdigest()
    assert db.has_inbound_integration_receipt(
        "canonical-session", platform_message_id="$human", integration="matrix-peer-buffer",
        receipt_id=integration["batch_id"], content_sha256=integration["content_sha256"],
    )
    db.close()
    assert "[Alice] human turn [authorized human reference]" in rows[-1]["content"]
    assert len(expanded_inputs) == 1
    assert "[Alice] human turn @file:human.txt" in expanded_inputs[0]
    assert "report.pdf" in expanded_inputs[0]
    assert "reply-secret" not in expanded_inputs[0]
    assert "peer-secret" not in expanded_inputs[0]
    assert "[authorized human reference]" in rows[-1]["content"]
    assert "report.pdf" in rows[-1]["content"]
    assert "literal @file:reply-secret.txt" in rows[-1]["content"]
    assert "literal @file:peer-secret.txt" in rows[-1]["content"]

    from hermes_cli.plugins import discover_plugins
    discover_plugins(force=True)
    model_tools.get_tool_definitions(enabled_toolsets=["hermes-matrix"])
    tokens = set_session_vars(
        platform="matrix", chat_id="!room:hs", session_key="matrix:route",
        session_id="canonical-session", message_id="$human",
    )
    try:
        acknowledged = model_tools.handle_function_call(
            "matrix_ack_peer_messages", {"batch_id": marker["integration"]["batch_id"]},
        )
    finally:
        clear_session_vars(tokens)
    assert '"ok": true' in acknowledged
    assert not adapter._peer_message_buffer.pending("!room:hs")["messages"]


@pytest.mark.asyncio
async def test_gateway_prepares_matrix_batch_after_hygiene_rotation(monkeypatch):
    """Exercise the real turn-preparation ordering across canonical-id rotation."""
    import gateway.run as gateway_run
    import gateway.run_turn as run_turn

    runner = run_turn.GatewayTurnMixin()
    entry = SimpleNamespace(session_id="pre-hygiene", session_key="matrix:route")
    event = SimpleNamespace(
        text="human", message_id="$human", raw_message={}, internal=False,
        auto_skill=None, channel_prompt=None, timestamp=None,
    )
    source = SimpleNamespace(platform=SimpleNamespace(value="matrix"), chat_id="!room:hs")
    observed = []

    class Adapter:
        async def prepare_inbound_event(self, prepared_event, canonical_session_id, turn_key):
            observed.append(canonical_session_id)
            prepared_event.raw_message["_persist_user_display_metadata"] = {"integration": {
                "batch_id": "batch", "inbound_event_id": turn_key,
            }}
            return prepared_event

    adapter = Adapter()
    runner.config = SimpleNamespace()
    history = [
        {"role": "user", "content": "1"}, {"role": "assistant", "content": "2"},
        {"role": "user", "content": "3"}, {"role": "assistant", "content": "4"},
    ]
    runner.async_session_store = SimpleNamespace(load_transcript=AsyncMock(return_value=history))
    runner._adapter_for_source = lambda _source: adapter
    runner._hmwa_open_session = AsyncMock(return_value=(False, False))
    runner._set_session_env = lambda context: [context.session_id]
    runner._clear_session_env = lambda _tokens: None
    runner._pinned_session_context_prompt = lambda *_args: "prompt"
    runner._hmwa_acquire_turn_lease = AsyncMock()
    runner._mark_durable_active_turn = AsyncMock()

    async def rotate(*_args):
        entry.session_id = "post-hygiene"
        return history

    runner._hmwa_run_session_hygiene = rotate
    runner._hmwa_first_contact_notes = AsyncMock()
    runner._voice_channel_sidecar_note = lambda *_args: None
    runner._prepare_profile_scoped_inbound_message_text = AsyncMock(return_value="human")
    runner._hmwa_apply_message_timestamp = lambda _event, text: (text, None, None)
    runner._bind_adapter_run_generation = lambda *_args: None
    monkeypatch.setattr(
        run_turn, "build_session_context",
        lambda _source, _config, session: SimpleNamespace(session_id=session.session_id),
    )
    monkeypatch.setattr(gateway_run, "_load_gateway_config", lambda: {})

    prepared, tokens = await runner._hmwa_prepare_turn(
        event, source, entry, "matrix:route", "quick", 7,
    )
    assert isinstance(prepared, runner._PreparedTurn)
    assert observed == ["post-hygiene"]
    assert tokens == ["post-hygiene"]
    assert prepared.persist_user_display_metadata["integration"]["batch_id"] == "batch"


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
    home = tmp_path / "profile"
    monkeypatch.setenv("HERMES_HOME", str(home))
    monkeypatch.setenv("MATRIX_ALLOWED_USERS", "@alice:example.org,@agent-b:example.org")
    monkeypatch.setenv("MATRIX_REQUIRE_MENTION", "true")
    monkeypatch.setenv("MATRIX_AUTO_THREAD", "false")
    adapter = _make_adapter(extra={
        "peer_agent_ids": ["@agent-b:example.org"],
        "peer_message_buffer": {"enabled": True, "auto_clear": True},
    })
    adapter._peer_message_buffer = MatrixPeerBuffer(
        home / "matrix_peer_messages.sqlite3", PeerBufferConfig(enabled=True, auto_clear=True))
    await adapter._on_room_message(_make_event("peer hello", sender="@agent-b:example.org", event_id="$blocked"))
    await adapter._on_room_message(_make_event("@hermes:example.org go", sender="@alice:example.org", event_id="$ok"))
    batch_event = adapter.handle_message.await_args.args[0]
    batch_event = await adapter.prepare_inbound_event(batch_event, "canonical-session", "$ok")
    canonical_content = adapter.attach_inbound_context(batch_event, batch_event.text)
    adapter.bind_inbound_persistence_content(batch_event, canonical_content)
    await adapter.on_processing_start(batch_event)
    assert adapter._peer_message_buffer.pending("!room1:example.org")["messages"]
    assert adapter._peer_message_buffer.claim_batch(
        room_id="!room1:example.org",
        batch_id=batch_event.raw_message["_matrix_peer_batch_id"],
        session_key="canonical-session", turn_key="$ok",
    )
    marker = batch_event.raw_message["_persist_user_display_metadata"]
    integration = marker["integration"]
    db = SessionDB(home / "state.db")
    db.create_session(session_id="canonical-session", source="matrix", model="test")
    db.append_message(
        session_id="canonical-session", role="user", content=canonical_content,
        platform_message_id="$ok", display_metadata=marker,
    )
    db.close()
    from gateway.response_filters import is_intentional_silence_agent_result
    batch_event.raw_message["_inbound_model_turn_succeeded"] = (
        is_intentional_silence_agent_result({"failed": False}, "NO_REPLY")
    )
    await adapter.on_processing_complete(batch_event, __import__("gateway.platforms.base", fromlist=["ProcessingOutcome"]).ProcessingOutcome.SUCCESS)
    assert not adapter._peer_message_buffer.pending("!room1:example.org")["messages"]


    await adapter._on_room_message(_make_event("peer retry", sender="@agent-b:example.org", event_id="$blocked2"))
    adapter.handle_message.side_effect = RuntimeError("turn failed")
    with pytest.raises(RuntimeError):
        await adapter._on_room_message(_make_event("@hermes:example.org retry", sender="@alice:example.org", event_id="$fail"))
    assert adapter._peer_message_buffer.pending("!room1:example.org")["messages"]


@pytest.mark.asyncio
async def test_durable_receipt_does_not_auto_clear_failed_no_reply_turn(tmp_path, monkeypatch):
    home = tmp_path / "profile"
    monkeypatch.setenv("HERMES_HOME", str(home))
    adapter = _make_adapter(extra={"peer_message_buffer": {"enabled": True, "auto_clear": True}})
    adapter._peer_message_buffer = MatrixPeerBuffer(
        home / "matrix_peer_messages.sqlite3", PeerBufferConfig(enabled=True, auto_clear=True))
    store = adapter._peer_message_buffer
    store.add(room_id="!room1:example.org", event_id="$peer", sender="@peer:hs", timestamp=1, body="x")
    batch = store.prepare_batch("!room1:example.org")
    assert batch and store.claim_batch(
        room_id="!room1:example.org", batch_id=batch["batch_id"],
        session_key="canonical", turn_key="$human",
    )
    augmented = "peer envelope\n\nhuman"
    content_hash = hashlib.sha256(augmented.encode()).hexdigest()
    assert store.bind_batch_content(
        room_id="!room1:example.org", batch_id=batch["batch_id"], content_sha256=content_hash,
    )
    marker = {"integration": {
        "integration": "matrix-peer-buffer", "batch_id": batch["batch_id"],
        "room_id": "!room1:example.org", "event_ids": ["$peer"],
        "inbound_event_id": "$human", "content_sha256": content_hash,
    }}
    db = SessionDB(home / "state.db")
    db.create_session(session_id="canonical", source="matrix", model="test")
    db.append_message(
        session_id="canonical", role="user", content=augmented,
        platform_message_id="$human", display_metadata=marker,
    )
    db.close()
    from gateway.response_filters import is_intentional_silence_agent_result
    event = SimpleNamespace(
        message_id="$human", source=SimpleNamespace(chat_id="!room1:example.org"),
        raw_message={
            "_matrix_peer_batch_id": batch["batch_id"],
            "_persisted_integration_receipt": True,
            "_inbound_model_turn_succeeded": is_intentional_silence_agent_result(
                {"failed": True}, "NO_REPLY",
            ),
        },
    )
    outcome = __import__("gateway.platforms.base", fromlist=["ProcessingOutcome"]).ProcessingOutcome.SUCCESS
    await adapter.on_processing_complete(event, outcome)
    assert store.pending("!room1:example.org")["messages"]
    assert store.prepare_batch("!room1:example.org") is None
    assert store.delivered_batches(room_id="!room1:example.org")[0]["batch_id"] == batch["batch_id"]


@pytest.mark.asyncio
async def test_failure_without_copied_receipt_reconciles_commit_and_releases_live_claim(tmp_path, monkeypatch):
    home = tmp_path / "profile"
    monkeypatch.setenv("HERMES_HOME", str(home))
    adapter = _make_adapter(extra={"peer_message_buffer": {"enabled": True}})
    adapter._peer_message_buffer = MatrixPeerBuffer(
        home / "matrix_peer_messages.sqlite3", PeerBufferConfig(enabled=True))
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
        raw_message={"_matrix_peer_batch_id": batch["batch_id"]},
    )
    outcome = __import__("gateway.platforms.base", fromlist=["ProcessingOutcome"]).ProcessingOutcome.FAILURE
    await adapter.on_processing_complete(event, outcome)
    retry = store.prepare_batch("!room1:example.org")
    assert retry and retry["batch_id"] == batch["batch_id"]
    assert store.claim_batch(
        room_id="!room1:example.org", batch_id=batch["batch_id"],
        session_key="canonical", turn_key="$retry",
    )


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
    canonical_content = adapter.attach_inbound_context(event, event.text)
    adapter.bind_inbound_persistence_content(event, canonical_content)
    marker = event.raw_message["_persist_user_display_metadata"]
    db = SessionDB(tmp_path / "profile" / "state.db")
    try:
        db.create_session(session_id="canonical-session", source="matrix", model="test")
        db.append_message(
            session_id="canonical-session", role="user", content=canonical_content,
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
    assert "event_id=$peer" in adapter.attach_inbound_context(prepared, prepared.text)


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
