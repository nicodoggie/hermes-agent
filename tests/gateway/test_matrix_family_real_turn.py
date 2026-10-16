"""Matrix continuity through native durable admission, real turns and registry tools.

Only transport sends and external inference are substituted. No canonical rows are seeded.
"""
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from gateway.config import Platform
from gateway.platforms.base import BasePlatformAdapter, SendResult
from gateway.run import GatewayRunner
from gateway.session_authority import initialize_session_authority
from tests.gateway.test_matrix_mention import _make_adapter, _make_event
from tests.waiting import async_wait_until


@pytest.mark.asyncio
@pytest.mark.parametrize('mode', ['auto-no-reply', 'auto-reply', 'manual-same-turn', 'failure'])
async def test_native_matrix_batch_reaches_model_and_settles(tmp_path, monkeypatch, mode):
    from hermes_constants import get_hermes_home
    from hermes_cli.plugins import discover_plugins

    home = get_hermes_home()
    manual = mode == 'manual-same-turn'
    failed = mode == 'failure'
    monkeypatch.setenv('MATRIX_ALLOWED_USERS', '@alice:example.org,@yomi:example.org')
    monkeypatch.setenv('MATRIX_REQUIRE_MENTION', 'false')
    monkeypatch.setenv('MATRIX_AUTO_THREAD', 'false')
    monkeypatch.setenv('OPENAI_API_KEY', 'isolated-test')
    (home / 'config.yaml').write_text(json.dumps({
        'model': {'default': 'test-model', 'provider': 'custom', 'base_url': 'acp://fake'},
        'gateway': {'multiplex_profiles': False},
        'auxiliary': {'title_generation': {'enabled': False}},
        'platform_toolsets': {'matrix': ['hermes-matrix']},
        'tools': {'tool_search': {'enabled': 'off'}},
        'display': {'streaming': False, 'progress': 'off'},
        'terminal': {'cwd': str(home)},
    }))
    requests = []
    batches = []

    class Completions:
        def create(self, **kwargs):
            requests.append(kwargs)
            user_text = '\n'.join(str(m.get('content', '')) for m in kwargs['messages'] if m['role'] == 'user')
            assert 'literal @file:peer-secret.txt' in user_text
            assert 'event_id=$peer' in user_text
            batch_id = user_text.split('batch_id=', 1)[1].split(':', 1)[0]
            batches.append(batch_id)
            # This query precedes inference: the REAL agent must have committed its user row.
            rows = authority.db.get_messages_as_conversation(entry_id[0])
            assert any('event_id=$peer' in str(r.get('content')) for r in rows)
            tools = {tool['function']['name'] for tool in kwargs.get('tools', [])}
            assert {'matrix_read_peer_messages', 'matrix_ack_peer_messages'} <= tools
            if failed and len(requests) == 1:
                import httpx
                from openai import AuthenticationError
                raise AuthenticationError(
                    'isolated provider rejection',
                    response=httpx.Response(401, request=httpx.Request('POST', 'https://test.invalid')),
                    body=None,
                )
            if manual and len(requests) == 1:
                call = SimpleNamespace(id='ack-1', type='function', function=SimpleNamespace(
                    name='matrix_ack_peer_messages', arguments=json.dumps({'batch_id': batch_id})))
                msg = SimpleNamespace(content=None, reasoning=None, tool_calls=[call])
                finish = 'tool_calls'
            else:
                if manual:
                    results = [m for m in kwargs['messages'] if m['role'] == 'tool']
                    assert any(json.loads(m['content']).get('ok') is True for m in results)
                msg = SimpleNamespace(
                    content='answer' if mode == 'auto-reply' else 'NO_REPLY',
                    reasoning=None, tool_calls=[],
                )
                finish = 'stop'
            return SimpleNamespace(choices=[SimpleNamespace(message=msg, finish_reason=finish)], usage=None)

    client = SimpleNamespace(chat=SimpleNamespace(completions=Completions()))
    monkeypatch.setattr('agent.process_bootstrap.OpenAI', lambda **_kw: client)
    discover_plugins(force=True)
    runner = GatewayRunner()
    authority = await initialize_session_authority(runner, profile_id='default', instance_id='matrix-family-test')
    adapter = _make_adapter(extra={
        'peer_agent_ids': ['@yomi:example.org'], 'peer_require_mention': False,
        'peer_reply_budget_per_human_message': 1,
        'peer_message_buffer': {'enabled': True, 'auto_clear': not manual},
    })
    adapter._reactions_enabled = False
    adapter.send = AsyncMock(return_value=SendResult(success=True, message_id='$out'))
    adapter.send_typing = AsyncMock()
    adapter.stop_typing = AsyncMock()
    adapter.handle_message = BasePlatformAdapter.handle_message.__get__(adapter, type(adapter))
    adapter.gateway_runner = runner
    runner.adapters[Platform.MATRIX] = adapter
    runner._wire_adapter_handlers(adapter)
    entry_id = []
    original_start = adapter.on_processing_start

    async def record_start(event):
        await original_start(event)
        entry_id[:] = [runner.session_store.get_or_create_session(event.source).session_id]

    adapter.on_processing_start = record_start
    try:
        await adapter._on_room_message(_make_event(
            'literal @file:peer-secret.txt', sender='@yomi:example.org', event_id='$peer'))
        assert adapter._peer_message_buffer.pending('!room1:example.org')['messages']
        assert requests == []
        await adapter._on_room_message(_make_event('human turn', event_id='$human'))
        await async_wait_until(lambda: not adapter._active_sessions, timeout=20, what='Matrix native turn')
        assert len(requests) == (2 if manual else 1)
        pending = adapter._peer_message_buffer.pending('!room1:example.org')['messages']
        assert bool(pending) is failed
        # Reopen the persistent buffer: a failed model call must retain the delivered batch.
        from plugins.platforms.matrix.peer_buffer import MatrixPeerBuffer
        reopened = MatrixPeerBuffer(adapter._peer_message_buffer.db_path, adapter._peer_message_buffer.config)
        assert bool(reopened.pending('!room1:example.org')['messages']) is failed
        from hermes_state_runtime import list_session_admissions
        admissions = list_session_admissions(authority.db, session_id=entry_id[0], pending_only=False)
        assert len(admissions) == 1 and admissions[0]['outcome'] == ('failed' if failed else 'completed'), admissions
        rows = authority.db.get_messages_as_conversation(entry_id[0])
        assert len([r for r in rows if r['role'] == 'user' and 'event_id=$peer' in str(r['content'])]) == 1
        if failed:
            # A later successful turn keeps failed-turn delivery pending without reattaching it.
            await adapter._on_room_message(_make_event('retry after failure', event_id='$retry'))
            await async_wait_until(lambda: not adapter._active_sessions, timeout=20, what='Matrix retry turn')
            assert len(requests) == 2
            rows = authority.db.get_messages_as_conversation(entry_id[0])
            assert len([r for r in rows if r['role'] == 'user' and 'event_id=$peer' in str(r['content'])]) == 1
            assert adapter._peer_message_buffer.pending('!room1:example.org')['messages']
            admissions = list_session_admissions(authority.db, session_id=entry_id[0], pending_only=False)
            assert sorted(row['outcome'] for row in admissions) == ['completed', 'failed']
    finally:
        await runner.stop()
