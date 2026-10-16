"""Skill palette and invocation parity at both ACP entry points, using canonical disk skills."""
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import acp
import pytest

from acp_adapter.gateway_server import GatewayACPAgent
from tests.acp_adapter.test_acp_commands import CaptureConn, make_agent_and_state
from tests.acp_adapter.test_control_runtime_context import RUNTIME_CONTEXT


def write_skill(root, name, body):
    path = root / name / 'SKILL.md'
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(f'---\nname: {name}\ndescription: Exercise {name}\n---\n{body}\n')
    return path


@pytest.fixture(params=['inprocess', 'gateway'])
def skill_session(request, tmp_path, monkeypatch):
    home = tmp_path / 'profile'
    home.mkdir()
    monkeypatch.setenv('HERMES_HOME', str(home))
    monkeypatch.setattr(Path, 'home', lambda: tmp_path)
    if request.param == 'inprocess':
        agent, state, runtime, conn = make_agent_and_state()
        state.cwd = str(tmp_path)
        return SimpleNamespace(agent=agent, sid=state.session_id, conn=conn, state=state,
                               home=home, runs=runtime.runs)
    agent = GatewayACPAgent()
    agent._conn, agent._gateway = CaptureConn(), AsyncMock()
    agent._snapshots['s'] = {'info': {'cwd': str(tmp_path)}}
    runs = []

    async def rpc(method, **params):
        if method == 'session.stats':
            return {}
        assert method == 'prompt.submit', method
        runs.append(params['text'])
        admission = str(len(runs))
        agent._terminals[admission] = {'outcome': 'completed'}
        return {'admission_id': admission}

    agent._gateway.rpc.side_effect = rpc
    return SimpleNamespace(agent=agent, sid='s', conn=agent._conn, state=None, home=home, runs=runs)


async def palette(session, cwd):
    if session.state is not None:
        session.state.cwd = str(cwd)
        await session.agent._send_available_commands_update(session.state)
    else:
        session.agent._snapshots[session.sid]['info']['cwd'] = str(cwd)
        await session.agent._after_attach(session.sid, session.sid)
    update = next(u for _, u in reversed(session.conn.updates)
                  if u.session_update == 'available_commands_update')
    return {c.name for c in update.available_commands}


@pytest.mark.asyncio
async def test_skill_palette_and_stacked_execution_follow_session_workspace(skill_session, tmp_path):
    s = skill_session
    projects = [tmp_path / name for name in ('one', 'two')]
    for project in projects:
        (project / '.git').mkdir(parents=True)
        write_skill(project / '.hermes' / 'skills', 'local-proof', 'BODY_' + project.name)
    (s.home / 'config.yaml').write_text(json.dumps({'skills': {'trusted_project_dirs': list(map(str, projects))}}))
    write_skill(s.home / 'skills', 'second-proof', 'SECOND_BODY')
    instruction = 'WS_SHARED first line\n  preserve indentation and --session\n<example>user XML</example>\n' + RUNTIME_CONTEXT
    for project in [*projects, projects[0]]:
        assert {'help', 'goal', 'local-proof', 'second-proof'} <= await palette(s, project)
        before = len(s.runs)
        result = await s.agent.prompt([acp.text_block('/local-proof /second-proof ' + instruction)], s.sid)
        assert result.stop_reason == 'end_turn'
        assert len(s.runs) == before + 1
        assert 'BODY_' + project.name in s.runs[-1] and 'SECOND_BODY' in s.runs[-1]
        assert instruction in s.runs[-1]
        assert 'BODY_' + ('two' if project.name == 'one' else 'one') not in s.runs[-1]


@pytest.mark.asyncio
async def test_skill_load_failure_is_local_and_native_and_unknown_routing_survive(skill_session, tmp_path, monkeypatch):
    s = skill_session
    skill = write_skill(s.home / 'skills', 'vanishing-proof', 'WILL_DISAPPEAR')
    write_skill(s.home / 'skills', 'help', 'MUST_NOT_SHADOW_NATIVE')
    names = await palette(s, tmp_path)
    assert 'vanishing-proof' in names
    skill.unlink()  # retain the canonical command cache, fail the actual local load
    reply = await s.agent.prompt([acp.text_block('/vanishing-proof do work')], s.sid)
    assert reply.stop_reason == 'end_turn' and not s.runs
    assert any('Failed to load skill' in getattr(getattr(u, 'content', None), 'text', '')
               for _, u in s.conn.updates)
    await s.agent.prompt([acp.text_block('/help')], s.sid)
    assert not s.runs
    await s.agent.prompt([acp.text_block('/notacommand keep\n  these lines')], s.sid)
    assert s.runs == ['/notacommand keep\n  these lines']
    monkeypatch.setattr('agent.skill_commands.get_skill_commands', lambda: (_ for _ in ()).throw(OSError('scan unavailable')))
    assert {'help', 'model', 'goal', 'reasoning'} <= await palette(s, tmp_path)


@pytest.mark.asyncio
async def test_gateway_skill_work_stays_in_owning_profile(tmp_path, monkeypatch):
    monkeypatch.setattr(Path, 'home', lambda: tmp_path)
    sessions = []
    for name in ('alpha', 'beta'):
        home = tmp_path / name
        home.mkdir()
        write_skill(home / 'skills', name + '-proof', 'PROFILE_' + name)
        monkeypatch.setenv('HERMES_HOME', str(home))
        agent = GatewayACPAgent()
        agent._conn, agent._gateway = CaptureConn(), AsyncMock()
        agent._snapshots[name] = {'info': {'cwd': str(tmp_path)}}
        agent._gateway.rpc.return_value = {'admission_id': 'a'}
        sessions.append(SimpleNamespace(agent=agent, sid=name, conn=agent._conn, state=None, home=home))
    # Process environment still names beta while alpha's deferred worker executes.
    for s in [*sessions, sessions[0]]:
        names = await palette(s, tmp_path)
        assert s.sid + '-proof' in names
        other = 'beta' if s.sid == 'alpha' else 'alpha'
        assert other + '-proof' not in names
        s.agent._terminals['a'] = {'outcome': 'completed'}
        await s.agent.prompt([acp.text_block('/' + s.sid + '-proof scoped instruction')], s.sid)
        submitted = s.agent._gateway.rpc.await_args.kwargs['text']
        assert 'PROFILE_' + s.sid in submitted and 'PROFILE_' + other not in submitted
    from hermes_constants import get_hermes_home
    assert get_hermes_home() == sessions[1].home


@pytest.mark.asyncio
async def test_skill_invocation_expands_even_before_palette_delivery(skill_session):
    s = skill_session
    write_skill(s.home / 'skills', 'cold-proof', 'COLD_SKILL_BODY')
    response = await s.agent.prompt([acp.text_block('/cold-proof first\n  second line')], s.sid)
    assert response.stop_reason == 'end_turn'
    assert len(s.runs) == 1
    assert 'COLD_SKILL_BODY' in s.runs[0] and 'first\n  second line' in s.runs[0]
