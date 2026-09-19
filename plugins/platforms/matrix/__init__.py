import json

from gateway.session_context import get_session_env
from hermes_constants import get_hermes_home

from .adapter import MatrixAdapter, register as register_platform
from .peer_buffer import MatrixPeerBuffer, PeerBufferConfig


def _ack_peer_messages(args: dict) -> str:
    """Acknowledge only the exact retained peer event IDs in this Matrix room."""
    if get_session_env("HERMES_SESSION_PLATFORM", "").lower() != "matrix":
        return json.dumps({"ok": False, "error": "Matrix session required"})
    room_id = get_session_env("HERMES_SESSION_CHAT_ID", "").strip()
    event_ids = args.get("event_ids") if isinstance(args, dict) else None
    if not room_id or not isinstance(event_ids, list) or not all(isinstance(value, str) for value in event_ids):
        return json.dumps({"ok": False, "error": "event_ids and a Matrix room session are required"})
    store = MatrixPeerBuffer(get_hermes_home() / "matrix_peer_messages.sqlite3", PeerBufferConfig(enabled=True))
    try:
        count = store.acknowledge(room_id=room_id, event_ids=event_ids)
    except ValueError as exc:
        return json.dumps({"ok": False, "error": str(exc)})
    return json.dumps({"ok": True, "room_id": room_id, "acknowledged": count, "event_ids": event_ids})


def register(ctx) -> None:
    register_platform(ctx)
    ctx.register_tool(
        name="matrix_ack_peer_messages",
        toolset="messaging",
        schema={
            "name": "matrix_ack_peer_messages",
            "description": "Acknowledge an exact batch of retained Matrix peer event IDs in the current room.",
            "parameters": {
                "type": "object",
                "properties": {"event_ids": {"type": "array", "items": {"type": "string"}}},
                "required": ["event_ids"],
            },
        },
        handler=_ack_peer_messages,
        description="Acknowledge exact retained Matrix peer messages for the current room.",
        emoji="📥",
    )


__all__ = ["MatrixAdapter", "register"]
