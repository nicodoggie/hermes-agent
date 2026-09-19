# Persistent Matrix Peer-Message Buffer Implementation Plan

**Goal:** Persist authorized peer messages that are gated before model invocation, then deliver them once in the next permitted Matrix turn without mutating transcript roles or executing quoted commands.

**Architecture:** Add a profile-scoped SQLite store under `get_hermes_home()`, keyed by Matrix room. It keeps bounded newest messages, records eviction omissions, returns stable batches without deleting, and acknowledges only exact IDs. The Matrix adapter buffers only after existing room/sender authorization and peer gating; a permitted turn appends a labeled, non-command peer-context block to its real inbound text. A plugin tool acknowledges an exact batch using the current Matrix session platform/chat scope.

**Defaults:** Buffer is opt-in (`peer_message_buffer.enabled: false`); `auto_clear: false`; count 100; byte cap 256 KiB; per-message cap 16 KiB UTF-8 with explicit truncation marker. Overflow eviction remains active whenever enabled.

### Task 1: Add SQLite persistence contract

- Create `plugins/platforms/matrix/peer_buffer.py` with schema initialization, atomic insert/retention, read-only delivery, exact scoped acknowledgement, and omission tracking.
- Test restart persistence, Unicode byte limits, overflow omission, concurrent writers, idempotent ack, and profile/room scope.

### Task 2: Integrate Matrix gating and delivery

- Construct the store from profile home and YAML settings.
- Buffer authorized peer events only when mention/budget gates reject them; leave unauthorized events unbuffered.
- On the next permitted turn, include oldest-to-newest retained messages and omission notice in a labeled context block; never normalize quoted content as a command.
- Keep the delivered batch pending until explicit ack; no model call occurs for gated events.

### Task 3: Register exact-batch acknowledgement tool and document configuration

- Register a Matrix-scoped plugin tool using session platform/chat IDs; reject missing/non-Matrix/cross-room scopes and unknown IDs, while making retries idempotent.
- Document YAML keys, bounds, delivery/ack semantics, and crash behavior.
- Run focused Matrix tests and the repository `scripts/run_tests.sh` suites, inspect the final diff, and commit only the task-owned paths.
