"""Skill advertisement and execution through real ACP stdio and authenticated gateway WS."""
import json

import pytest

from tests.acp_adapter.test_control_runtime_context import RUNTIME_CONTEXT
from tests.acp_adapter.test_gateway_session_metadata import _reply, _updates
from tests.acp_adapter.test_gateway_sessions import editor, viewer
from tests.acp_adapter.test_skill_gateway_integration import write_skill
from tests.waiting import async_wait_until


@pytest.mark.platforms('linux')
@pytest.mark.asyncio
async def test_skills_are_advertised_and_executed_by_gateway_owner(daemon, tmp_path, model_peer):
    home = daemon[0]
    skill = write_skill(home / 'skills', 'wire-proof', 'WIRE_SKILL_BODY WS_SHARED')
    write_skill(home / 'skills', 'second-proof', 'SECOND_WIRE_BODY')
    vanished = write_skill(home / 'skills', 'vanishing-proof', 'NEVER_EXECUTED')
    config_before = (home / 'config.yaml').read_bytes()
    instruction = 'first line\n  keep indentation and --session\n<example>user XML</example>\n' + RUNTIME_CONTEXT
    async with viewer(daemon) as ws, editor(daemon, tmp_path) as acp:
        await acp.rpc('initialize', protocolVersion=1, clientCapabilities={})
        sid = (await acp.rpc('session/new', cwd=str(tmp_path), mcpServers=[]))['result']['sessionId']
        await async_wait_until(lambda: _updates(acp, sid, 'available_commands_update'),
                               what='skill palette after session/new')
        advertised = {c['name'] for c in _updates(acp, sid, 'available_commands_update')[-1]['availableCommands']}
        assert {'wire-proof', 'second-proof', 'vanishing-proof', 'goal', 'reasoning'} <= advertised
        vanished.unlink()
        assert 'Failed to load skill' in await _reply(acp, sid, '/vanishing-proof run this')
        assert model_peer.requests == []
        for command in ('/help', '/reasoning status', '/goal status'):
            await _reply(acp, sid, command + '\n' + RUNTIME_CONTEXT)
        assert model_peer.requests == []
        assert 'LOCAL_ACK_WS_SHARED' in await _reply(acp, sid, '/wire-proof /second-proof ' + instruction)
        assert len(model_peer.requests) == 1
        snapshot = await ws.rpc('session.resume', session_id=sid)
        user = next(m['content'] for m in snapshot['messages'] if m['role'] == 'user')
        assert 'WIRE_SKILL_BODY' in user and 'SECOND_WIRE_BODY' in user and instruction in user
        assert str(skill.parent) in user
        model_user = next(m['content'] for m in reversed(model_peer.requests[0]['messages']) if m['role'] == 'user')
        assert 'WIRE_SKILL_BODY' in json.dumps(model_user) and 'SECOND_WIRE_BODY' in json.dumps(model_user)
        assert 'LOCAL_ACK_WS_SHARED' in await _reply(acp, sid, '/wire-proof ' + instruction)
        assert len(model_peer.requests) == 2
        single = await ws.rpc('session.resume', session_id=sid)
        single_user = [m['content'] for m in single['messages'] if m['role'] == 'user'][-1]
        assert 'WIRE_SKILL_BODY' in single_user and instruction in single_user
        unknown = '/unrecognized-proof WS_SHARED\n  keep these lines'
        notice = await _reply(acp, sid, unknown)
        assert 'unrecognized-proof' in notice
        snapshot = await ws.rpc('session.resume', session_id=sid)
        # The gateway owns unknown-command notices; the adapter still submits the original text.
        assert snapshot['messages'] == single['messages']
        assert len(model_peer.requests) == 2
        for method in ('session/load', 'session/resume', 'session/fork'):
            before = len(acp.frames)
            result = await acp.rpc(method, sessionId=sid, cwd=str(tmp_path), mcpServers=[])
            attached = result['result'].get('sessionId', sid)
            await async_wait_until(lambda: _updates(acp, attached, 'available_commands_update', before),
                                   what=method + ' skill palette')
            names = {c['name'] for c in _updates(acp, attached, 'available_commands_update', before)[-1]['availableCommands']}
            assert 'wire-proof' in names
        print('ACP_SKILL_GATEWAY_RECEIPT=' + json.dumps({'session_id': sid,
              'advertised': sorted(advertised), 'provider_calls': len(model_peer.requests),
              'load_failure_local': True, 'stacked_skill_body_in_canonical_history': True,
              'multiline_instruction_preserved': True, 'gateway_unknown_notice_preserved': True}))
    assert (home / 'config.yaml').read_bytes() == config_before
