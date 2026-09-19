"""Matrix platform plugin with a deferred inbound adapter and lightweight client tools."""

from .peer_buffer import MatrixPeerBuffer, PeerBufferConfig
from .tools import _ack_peer_messages


def register(ctx) -> None:
    from .adapter import MatrixAdapter, register as register_platform
    from .tools import register_tools

    register_platform(ctx)
    register_tools(ctx)


def __getattr__(name):
    if name == "MatrixAdapter":
        from .adapter import MatrixAdapter
        return MatrixAdapter
    raise AttributeError(name)


__all__ = ["MatrixAdapter", "MatrixPeerBuffer", "PeerBufferConfig", "register"]
