"""ACP skill projection using the canonical discovery cache and invocation builders."""
from __future__ import annotations

from contextlib import contextmanager
import logging

from acp.schema import AvailableCommand, UnstructuredCommandInput

logger = logging.getLogger(__name__)


class SkillCommandLoadError(RuntimeError):
    pass


@contextmanager
def _skill_scope(cwd, home):
    from agent.runtime_cwd import reset_session_cwd, set_session_cwd
    from hermes_constants import reset_hermes_home_override, set_hermes_home_override

    cwd_token = set_session_cwd(cwd)
    home_token = set_hermes_home_override(home) if home is not None else None
    try:
        yield
    finally:
        if home_token is not None:
            reset_hermes_home_override(home_token)
        reset_session_cwd(cwd_token)


def available_skill_commands(cwd, native_commands, *, home=None):
    """Append installed skill rows; a broken scan leaves the native roster available."""
    commands = []
    with _skill_scope(cwd, home):
        from agent.skill_commands import get_skill_commands

        try:
            skill_commands = get_skill_commands()
        except Exception:
            logger.warning("Skill command scan failed; advertising built-in ACP commands only", exc_info=True)
            skill_commands = {}
        for key, info in skill_commands.items():
            name = key.lstrip("/")
            if name in native_commands:
                continue
            commands.append(AvailableCommand(
                name=name,
                description=info.get("description") or f"Invoke the {info.get('name') or name} skill",
                input=UnstructuredCommandInput(hint="optional instruction"),
            ))
    return commands


def expand_skill_command(text, cwd, session_id, *, home=None):
    """Expand one local skill invocation; None preserves ordinary unknown slash text."""
    parts = text.split(maxsplit=1)
    command = parts[0]
    instruction = parts[1] if len(parts) > 1 else ""
    from agent.skill_commands import (
        build_skill_invocation_message,
        build_stacked_skill_invocation_message,
        resolve_skill_command_key,
        split_stacked_skill_commands,
    )

    with _skill_scope(cwd, home):
        key = resolve_skill_command_key(command.lstrip("/").lower())
        if key is None:
            return None
        extra_keys, user_instruction = split_stacked_skill_commands(instruction)
        if extra_keys:
            stacked = build_stacked_skill_invocation_message(
                [key, *extra_keys], user_instruction, task_id=session_id
            )
            if stacked:
                return stacked[0]
            raise SkillCommandLoadError(f"Failed to load stacked skills starting with {key}")
        expanded = build_skill_invocation_message(key, user_instruction, task_id=session_id)
        if expanded is None:
            raise SkillCommandLoadError(f"Failed to load skill for {key}")
        return expanded
