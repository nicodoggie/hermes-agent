"""Authorization callback invocation shared by platform adapters."""
from __future__ import annotations

import inspect
import logging
from typing import TYPE_CHECKING, Any, Optional

from gateway.session import SessionSource

if TYPE_CHECKING:
    from gateway.platforms.base import BasePlatformAdapter

logger = logging.getLogger(__name__)


def check_sender_authorization(
    adapter: BasePlatformAdapter,
    user_id: Optional[str], chat_type: Optional[str], chat_id: Optional[str], *,
    is_bot: bool = False, thread_id: Optional[str] = None,
    source: Optional[SessionSource] = None,
) -> Optional[bool]:
    """Return literal booleans or unknown; forward full sources only to compatible callbacks."""
    callback = adapter._authorization_check
    if not user_id or callback is None:
        return None
    extra: dict[str, Any] = {}
    if is_bot:
        extra["is_bot"] = True
    if thread_id is not None:
        extra["thread_id"] = thread_id
    try:
        if source is not None:
            parameters = inspect.signature(callback).parameters
            if "source" in parameters or any(
                    p.kind == inspect.Parameter.VAR_KEYWORD for p in parameters.values()):
                extra["source"] = source
        result = callback(user_id, chat_type, chat_id, **extra)
    except Exception:
        logger.warning("[%s] Authorization check raised for user %s; treating as unknown",
                       adapter.name, user_id, exc_info=True)
        return None
    if result is True or result is False:
        return result
    logger.warning("[%s] Authorization check returned %s for user %s; treating as unknown",
                   adapter.name, type(result).__name__, user_id)
    return None


def canonicalize_source(adapter, source: Optional[SessionSource]):
    """Pin inbound routing identity before adapter authorization or key derivation."""
    if source is None:
        return None
    from gateway.session_identity import canonical_identity, identity_of
    identity = identity_of(source)
    if identity is not None:
        return identity
    runner = getattr(adapter, "gateway_runner", None)
    if runner is None or not callable(getattr(runner, "_transport_owner", None)):
        return None
    try:
        return canonical_identity(
            source, runner=runner, adapter=adapter, transport_profile=adapter._owner_transport_profile())
    except Exception:
        # Duck-typed runners (SimpleNamespace / MagicMock rigs) have no registry to resolve
        # against; the key then falls back to the pre-identity readers instead of failing ingress.
        logger.debug("[%s] identity resolution failed; using legacy key readers", adapter.name, exc_info=True)
        return None
