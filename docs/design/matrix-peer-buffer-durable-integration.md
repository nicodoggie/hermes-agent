# Matrix peer-buffer durable integration

## Boundary and marker

The Matrix adapter remains responsible for authorization, peer buffering, and batch
reservation. It must classify ordinary gateway commands before peer augmentation. A
reserved batch is attached to the `MessageEvent` as a generic inbound integration
receipt, not as Matrix-specific core state:

```json
{
  "integration": "matrix-peer-buffer",
  "batch_id": "...",
  "room_id": "...",
  "event_ids": ["..."],
  "reservation_generation": 1,
  "inbound_event_id": "$matrix-event",
  "content_sha256": "sha256-of-exact-persisted-user-content"
}
```

The gateway forwards this receipt through the existing generic
`persist_user_display_metadata` path. The staged user row therefore carries the
receipt in `messages.display_metadata`, in the same `SessionDB.append_messages_batch`
transaction as the exact clean user row and its `api_content` sidecar. No callback
or second SQLite database is treated as the commit point.

## Commit and recovery proof

`build_turn_context()` calls `_persist_turn_start()` before the first model request.
The normal and Codex paths use that same SessionDB transaction. The receipt is
queryable after a process crash with a narrow lookup by canonical session ID and
platform event ID, followed by exact receipt and content-hash verification. If the
transaction fails, neither the user row nor receipt is durable and the Matrix batch
remains retryable. If it commits, restart recovery can find the receipt and settle
or retry the same batch without appending another user row; platform message-id
and batch-id deduplication make recovery idempotent.

The adapter's lifecycle hooks are only settlement signals after the persisted row
has been proven to contain the receipt. Completion may auto-clear only when that
inclusion proof exists and the model turn completed successfully, including
`NO_REPLY`. Manual acknowledgement is allowed only after inclusion proof and is
idempotent for an already acknowledged batch.

## State and bounds

Batch reservation is serialized on the canonical resolved `SessionEntry.session_id`
and turn identity, after any pre-turn hygiene rotation and before context attachment.
A losing claim does not attach or process the batch. Delivered-but-unacknowledged
batches are not automatically prefixed again; a bounded Matrix plugin tool can list
and read them in the same profile and room after a canonical session transition. The
original canonical receipt remains the delivery proof, while an exact read atomically
authorizes the current canonical session to acknowledge that batch. A per-process
claim owner permits reclaim after restart only when no exact canonical receipt exists;
a live process cannot steal its active claim.
Preparation never creates a duplicate append. Event IDs are replay
safe and bounded metadata is pruned with the bounded batch ledger. Omission is
represented as a per-reservation generation/delta, settled exactly once across
delivery and later acknowledgement, so
repeated snapshots do not repeat notices.

Commands are classified before augmentation and retain existing peer authorization
gates. Manual ack deliberately remains acknowledged even if the later response
fails. Auto-clear requires both durable inclusion and successful model completion;
handler success alone is insufficient.
