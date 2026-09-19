import json

from gateway.session_context import get_session_env
from hermes_constants import get_hermes_home

from .adapter import MatrixAdapter, register as register_platform
from .peer_buffer import MatrixPeerBuffer, PeerBufferConfig


def _ack_peer_messages(args: dict) -> str:
    """Acknowledge one exact, previously prepared Matrix peer batch."""
    if get_session_env("HERMES_SESSION_PLATFORM", "").lower() != "matrix":
        return json.dumps({"ok": False, "error": "Matrix session required"})
    room_id = get_session_env("HERMES_SESSION_CHAT_ID", "").strip()
    batch_id = args.get("batch_id") if isinstance(args, dict) else None
    if not room_id or not isinstance(batch_id, str) or not batch_id:
        return json.dumps({"ok": False, "error": "batch_id and a Matrix room session are required"})
    store = MatrixPeerBuffer(get_hermes_home() / "matrix_peer_messages.sqlite3", PeerBufferConfig(enabled=True))
    try:
        count = store.acknowledge_batch(room_id=room_id, batch_id=batch_id)
    except ValueError as exc:
        return json.dumps({"ok": False, "error": str(exc)})
    return json.dumps({"ok": True, "room_id": room_id, "acknowledged": count, "batch_id": batch_id})


def register(ctx) -> None:
    register_platform(ctx)
    ctx.register_tool(
        name="matrix_ack_peer_messages",
        toolset="hermes-matrix",
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


__all__ = ["MatrixAdapter", "register"]
