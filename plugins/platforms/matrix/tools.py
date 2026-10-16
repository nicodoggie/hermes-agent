"""Lightweight Matrix client tools; the inbound adapter remains deferred."""
import json

from gateway.session_context import get_session_env
from hermes_constants import get_hermes_home
from hermes_state import SessionDB

from .peer_buffer import MatrixPeerBuffer, PeerBufferConfig


def _ack_peer_messages(args: dict, **_kwargs) -> str:
    """Acknowledge one exact, previously prepared Matrix peer batch."""
    if get_session_env("HERMES_SESSION_PLATFORM", "").lower() != "matrix":
        return json.dumps({"ok": False, "error": "Matrix session required"})
    room_id = get_session_env("HERMES_SESSION_CHAT_ID", "").strip()
    session_id = get_session_env("HERMES_SESSION_ID", "").strip()
    batch_id = args.get("batch_id") if isinstance(args, dict) else None
    if not room_id or not session_id or not isinstance(batch_id, str) or not batch_id:
        return json.dumps({"ok": False, "error": "batch_id and a canonical Matrix room session are required"})
    store = MatrixPeerBuffer(get_hermes_home() / "matrix_peer_messages.sqlite3", PeerBufferConfig(enabled=True))
    try:
        receipt = store.batch_receipt(room_id=room_id, batch_id=batch_id)
        if receipt is None:
            raise ValueError("unknown or undelivered Matrix peer batch")
        session_db = SessionDB(get_hermes_home() / "state.db")
        try:
            committed = session_db.has_inbound_integration_receipt(
                receipt["session_id"],
                platform_message_id=receipt["inbound_event_id"],
                integration="matrix-peer-buffer", receipt_id=batch_id,
                content_sha256=receipt["content_sha256"],
            )
        finally:
            session_db.close()
        if not committed:
            raise ValueError("unknown or undelivered Matrix peer batch")
        store.mark_included(room_id=room_id, batch_id=batch_id)
        count = store.acknowledge_batch(
            room_id=room_id,
            batch_id=batch_id,
            session_key=session_id,
        )
    except ValueError as exc:
        return json.dumps({"ok": False, "error": str(exc)})
    return json.dumps({"ok": True, "room_id": room_id, "acknowledged": count, "batch_id": batch_id})


def _read_peer_messages(args: dict, **_kwargs) -> str:
    """Discover or read delivered, unacknowledged batches in the current Matrix room."""
    if get_session_env("HERMES_SESSION_PLATFORM", "").lower() != "matrix":
        return json.dumps({"ok": False, "error": "Matrix session required"})
    room_id = get_session_env("HERMES_SESSION_CHAT_ID", "").strip()
    session_id = get_session_env("HERMES_SESSION_ID", "").strip()
    if not room_id or not session_id:
        return json.dumps({"ok": False, "error": "a canonical Matrix room session is required"})
    store = MatrixPeerBuffer(get_hermes_home() / "matrix_peer_messages.sqlite3", PeerBufferConfig(enabled=True))
    batch_id = args.get("batch_id") if isinstance(args, dict) else None
    before_batch_id = args.get("before_batch_id") if isinstance(args, dict) else None
    if not batch_id:
        candidates = store.delivered_batches(
            room_id=room_id, limit=21,
            before_batch_id=before_batch_id if isinstance(before_batch_id, str) else None,
        )
        page = candidates[:20]
        return json.dumps({
            "ok": True,
            "room_id": room_id,
            "batches": [
                {k: row[k] for k in ("batch_id", "event_count", "omitted_count")}
                for row in page
            ],
            "next_before_batch_id": page[-1]["batch_id"] if len(candidates) > 20 else None,
        })
    candidate = store.delivered_batch(room_id=room_id, batch_id=batch_id)
    if candidate is None:
        return json.dumps({"ok": False, "error": "unknown or undelivered Matrix peer batch"})
    session_db = SessionDB(get_hermes_home() / "state.db")
    try:
        committed = session_db.has_inbound_integration_receipt(
            candidate["session_id"],
            platform_message_id=candidate["inbound_event_id"],
            integration="matrix-peer-buffer", receipt_id=batch_id,
            content_sha256=candidate["content_sha256"],
        )
    finally:
        session_db.close()
    if not committed:
        return json.dumps({"ok": False, "error": "unverified Matrix peer batch receipt"})
    batch = store.read_delivered_batch(
        room_id=room_id, batch_id=batch_id, session_key=session_id,
    )
    if batch is None:
        return json.dumps({"ok": False, "error": "unknown or undelivered Matrix peer batch"})
    return json.dumps({"ok": True, "room_id": room_id, **batch}, ensure_ascii=False)


def register_tools(ctx) -> None:
    ctx.register_tool(
        name="matrix_read_peer_messages",
        toolset="matrix",
        schema={
            "name": "matrix_read_peer_messages",
            "description": "List or read retained delivered Matrix peer batches in the current room.",
            "parameters": {
                "type": "object",
                "properties": {
                    "batch_id": {"type": "string"},
                    "before_batch_id": {
                        "type": "string",
                        "description": "Continue discovery before the prior page's last batch ID.",
                    },
                },
            },
        },
        handler=_read_peer_messages,
        description="Read exact retained Matrix peer messages for the current room.",
        emoji="📖",
    )
    ctx.register_tool(
        name="matrix_ack_peer_messages",
        toolset="matrix",
        schema={
            "name": "matrix_ack_peer_messages",
            "description": "Acknowledge an exact delivered Matrix peer batch in the current room.",
            "parameters": {
                "type": "object",
                "properties": {"batch_id": {"type": "string"}},
                "required": ["batch_id"],
            },
        },
        handler=_ack_peer_messages,
        description="Acknowledge exact retained Matrix peer messages for the current room.",
        emoji="📥",
    )
