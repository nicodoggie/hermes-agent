"""Durable, profile-scoped buffering for Matrix peer messages."""
from __future__ import annotations

import hashlib
import json
import sqlite3
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


_PROCESS_CLAIM_OWNER = uuid.uuid4().hex


@dataclass(frozen=True)
class PeerBufferConfig:
    enabled: bool = False
    auto_clear: bool = False
    max_count: int = 100
    max_bytes: int = 256 * 1024
    max_message_bytes: int = 16 * 1024

    @classmethod
    def from_mapping(cls, raw: object) -> PeerBufferConfig:
        data = raw if isinstance(raw, dict) else {}

        def boolean(name: str, default: bool) -> bool:
            value = data.get(name, default)
            if isinstance(value, str):
                return value.strip().lower() not in {"false", "0", "no", "off"}
            return bool(value)

        def integer(name: str, default: int, minimum: int = 1) -> int:
            try:
                return max(minimum, int(data.get(name, default)))
            except (TypeError, ValueError):
                return default

        return cls(
            enabled=boolean("enabled", False),
            auto_clear=boolean("auto_clear", False),
            max_count=integer("max_count", 100),
            max_bytes=integer("max_bytes", 256 * 1024),
            max_message_bytes=integer("max_message_bytes", 16 * 1024),
        )


class MatrixPeerBuffer:
    """SQLite store whose database path is supplied by the active Hermes profile."""

    def __init__(self, db_path: Path, config: PeerBufferConfig):
        self.db_path = Path(db_path)
        self.config = config
        if config.enabled:
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
            with self._connect() as db:
                db.executescript(
                    """
                    PRAGMA journal_mode=WAL;
                    CREATE TABLE IF NOT EXISTS peer_messages (
                        room_id TEXT NOT NULL, event_id TEXT NOT NULL, sender TEXT NOT NULL,
                        timestamp REAL NOT NULL, body TEXT NOT NULL, body_bytes INTEGER NOT NULL,
                        delivered INTEGER NOT NULL DEFAULT 0,
                        PRIMARY KEY (room_id, event_id)
                    );
                    CREATE TABLE IF NOT EXISTS peer_buffer_meta (
                        room_id TEXT PRIMARY KEY, omitted_count INTEGER NOT NULL DEFAULT 0,
                        delivered_omitted_count INTEGER NOT NULL DEFAULT 0
                    );
                    CREATE TABLE IF NOT EXISTS peer_batches (
                        batch_id TEXT PRIMARY KEY, room_id TEXT NOT NULL,
                        event_ids TEXT NOT NULL, omitted_count INTEGER NOT NULL,
                        state TEXT NOT NULL CHECK(state IN ('in_flight', 'delivered', 'acknowledged')),
                        claimed INTEGER NOT NULL DEFAULT 0,
                        session_key TEXT,
                        turn_key TEXT,
                        ack_count INTEGER NOT NULL DEFAULT 0,
                        reservation_generation INTEGER NOT NULL DEFAULT 0,
                        included INTEGER NOT NULL DEFAULT 0,
                        content_sha256 TEXT,
                        claim_owner TEXT,
                        read_session_key TEXT,
                        omission_settled INTEGER NOT NULL DEFAULT 0,
                        created_at REAL NOT NULL DEFAULT (strftime('%s','now'))
                    );
                    """
                )
                columns = {row[1] for row in db.execute("PRAGMA table_info(peer_messages)")}
                if "delivered" not in columns:
                    db.execute("ALTER TABLE peer_messages ADD COLUMN delivered INTEGER NOT NULL DEFAULT 0")
                meta_columns = {row[1] for row in db.execute("PRAGMA table_info(peer_buffer_meta)")}
                if "delivered_omitted_count" not in meta_columns:
                    db.execute(
                        "ALTER TABLE peer_buffer_meta ADD COLUMN delivered_omitted_count INTEGER NOT NULL DEFAULT 0"
                    )
                batch_columns = {row[1] for row in db.execute("PRAGMA table_info(peer_batches)")}
                for name, definition in (
                    ("claimed", "INTEGER NOT NULL DEFAULT 0"),
                    ("session_key", "TEXT"),
                    ("turn_key", "TEXT"),
                    ("ack_count", "INTEGER NOT NULL DEFAULT 0"),
                    ("reservation_generation", "INTEGER NOT NULL DEFAULT 0"),
                    ("included", "INTEGER NOT NULL DEFAULT 0"),
                    ("content_sha256", "TEXT"),
                    ("claim_owner", "TEXT"),
                    ("read_session_key", "TEXT"),
                    ("omission_settled", "INTEGER NOT NULL DEFAULT 0"),
                    ("created_at", "REAL NOT NULL DEFAULT 0"),
                ):
                    if name not in batch_columns:
                        db.execute(f"ALTER TABLE peer_batches ADD COLUMN {name} {definition}")
                # Older schemas already advanced omission progress when entering either terminal
                # state. Mark those snapshots settled so a later acknowledgement cannot replay it.
                db.execute(
                    "UPDATE peer_batches SET omission_settled=1 "
                    "WHERE state IN ('delivered','acknowledged') AND omission_settled=0"
                )

    def _connect(self) -> sqlite3.Connection:
        db = sqlite3.connect(self.db_path, timeout=10)
        db.row_factory = sqlite3.Row
        return db

    def add(self, *, room_id: str, event_id: str, sender: str, timestamp: float, body: str) -> bool:
        if not self.config.enabled or not room_id or not event_id:
            return False
        raw = str(body)
        encoded = raw.encode("utf-8")
        if len(encoded) > self.config.max_message_bytes:
            cap = self.config.max_message_bytes
            marker = "… [message truncated]".encode()
            prefix = encoded[: max(0, cap - len(marker))]
            while True:
                try:
                    prefix_text = prefix.decode("utf-8")
                    break
                except UnicodeDecodeError:
                    prefix = prefix[:-1]
            encoded = (prefix_text.encode("utf-8") + marker)[:cap]
            while True:
                try:
                    body = encoded.decode("utf-8")
                    break
                except UnicodeDecodeError:
                    encoded = encoded[:-1]
            encoded = body.encode("utf-8")
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            inserted = db.execute(
                "INSERT OR IGNORE INTO peer_messages "
                "(room_id,event_id,sender,timestamp,body,body_bytes) VALUES (?,?,?,?,?,?)",
                (room_id, event_id, sender, float(timestamp), body, len(encoded)),
            ).rowcount == 1
            if not inserted:
                db.commit()
                return False
            db.execute("INSERT OR IGNORE INTO peer_buffer_meta(room_id) VALUES (?)", (room_id,))
            rows = db.execute(
                "SELECT room_id,event_id,body_bytes FROM peer_messages "
                "WHERE room_id=? ORDER BY timestamp,event_id", (room_id,)
            ).fetchall()
            total = sum(int(row["body_bytes"]) for row in rows)
            evicted = 0
            while len(rows) > self.config.max_count or total > self.config.max_bytes:
                row = rows.pop(0)
                db.execute("DELETE FROM peer_messages WHERE room_id=? AND event_id=?", (row["room_id"], row["event_id"]))
                total -= int(row["body_bytes"])
                evicted += 1
            if evicted:
                db.execute(
                    "UPDATE peer_buffer_meta SET omitted_count=omitted_count+? WHERE room_id=?",
                    (evicted, room_id),
                )
            db.commit()
        return True

    def pending(self, room_id: str) -> dict:
        if not self.config.enabled:
            return {"messages": [], "omitted_count": 0}
        with self._connect() as db:
            rows = db.execute(
                "SELECT room_id,event_id,sender,timestamp,body FROM peer_messages "
                "WHERE room_id=? ORDER BY timestamp,event_id", (room_id,)
            ).fetchall()
            meta = db.execute(
                "SELECT omitted_count,delivered_omitted_count FROM peer_buffer_meta WHERE room_id=?", (room_id,)
            ).fetchone()
        return {
            "messages": [dict(row) for row in rows],
            "omitted_count": int(meta[0]) if meta else 0,
            "delivered_omitted_count": int(meta[1]) if meta else 0,
        }

    def prepare_batch(self, room_id: str) -> dict | None:
        """Return one durable batch of not-yet-successfully-delivered data."""
        if not self.config.enabled:
            return None
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            existing = db.execute(
                "SELECT batch_id,event_ids,omitted_count FROM peer_batches "
                "WHERE room_id=? AND state='in_flight' ORDER BY rowid DESC LIMIT 1", (room_id,)
            ).fetchone()
            if existing:
                return {
                    "batch_id": existing["batch_id"],
                    "event_ids": json.loads(existing["event_ids"]),
                    "omitted_count": int(existing["omitted_count"]),
                }
            rows = db.execute(
                "SELECT event_id FROM peer_messages WHERE room_id=? AND delivered=0 ORDER BY timestamp,event_id",
                (room_id,),
            ).fetchall()
            meta = db.execute(
                "SELECT omitted_count,delivered_omitted_count FROM peer_buffer_meta WHERE room_id=?", (room_id,)
            ).fetchone()
            omitted = max(0, int(meta[0]) - int(meta[1])) if meta else 0
            event_ids = [row["event_id"] for row in rows]
            if not event_ids and not omitted:
                db.commit()
                return None
            signature = json.dumps([room_id, event_ids, omitted], ensure_ascii=False, separators=(",", ":"))
            batch_id = "matrix-peer-" + hashlib.sha256(signature.encode("utf-8")).hexdigest()[:32]
            db.execute(
                "INSERT OR IGNORE INTO peer_batches "
                "(batch_id,room_id,event_ids,omitted_count,state,claimed) VALUES (?,?,?,?,?,0)",
                (batch_id, room_id, json.dumps(event_ids), omitted, "in_flight"),
            )
            terminal = db.execute(
                "SELECT rowid,event_ids,state FROM peer_batches WHERE room_id=? "
                "AND state IN ('delivered','acknowledged') ORDER BY rowid DESC",
                (room_id,),
            ).fetchall()
            removable = []
            for row in terminal[256:]:
                ids = json.loads(row["event_ids"])
                retained = 0
                if ids:
                    placeholders = ",".join("?" for _ in ids)
                    retained = db.execute(
                        f"SELECT count(*) FROM peer_messages WHERE room_id=? AND event_id IN ({placeholders})",
                        (room_id, *ids),
                    ).fetchone()[0]
                if row["state"] == "acknowledged" or not retained:
                    removable.append(row["rowid"])
            if removable:
                db.executemany("DELETE FROM peer_batches WHERE rowid=?", ((rowid,) for rowid in removable))
            db.commit()
        return {"batch_id": batch_id, "event_ids": event_ids, "omitted_count": omitted}

    def claim_batch(self, *, room_id: str, batch_id: str, session_key: str, turn_key: str) -> bool:
        """Atomically expose a prepared batch to one durable gateway turn."""
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            batch = db.execute(
                "SELECT state,claimed,session_key,turn_key,claim_owner FROM peer_batches "
                "WHERE batch_id=? AND room_id=?", (batch_id, room_id)
            ).fetchone()
            if not batch or batch["state"] != "in_flight":
                db.rollback()
                return False
            if batch["claimed"]:
                same = (
                    batch["session_key"] == session_key and batch["turn_key"] == turn_key
                    and batch["claim_owner"] == _PROCESS_CLAIM_OWNER
                )
                db.commit()
                return same
            db.execute(
                "UPDATE peer_batches SET claimed=1,session_key=?,turn_key=?,claim_owner=? "
                "WHERE batch_id=? AND room_id=? AND state='in_flight' AND claimed=0",
                (session_key, turn_key, _PROCESS_CLAIM_OWNER, batch_id, room_id),
            )
            db.commit()
        return True

    def reclaim_batch(
        self, *, room_id: str, batch_id: str, session_key: str, turn_key: str,
    ) -> bool:
        """Reclaim only a claim created by a different (dead) gateway process."""
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            updated = db.execute(
                "UPDATE peer_batches SET session_key=?,turn_key=?,claim_owner=?,content_sha256=NULL "
                "WHERE room_id=? AND batch_id=? AND state='in_flight' AND claimed=1 "
                "AND (claim_owner IS NULL OR claim_owner<>?)",
                (session_key, turn_key, _PROCESS_CLAIM_OWNER, room_id, batch_id, _PROCESS_CLAIM_OWNER),
            ).rowcount
            db.commit()
        return bool(updated)

    def complete_batch(self, *, room_id: str, batch_id: str, success: bool, auto_clear: bool = False) -> bool:
        """Settle only a batch previously prepared for this room; failed turns remain retryable."""
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            batch = db.execute(
                "SELECT event_ids,omitted_count,state,claimed,ack_count,included,omission_settled "
                "FROM peer_batches WHERE batch_id=? AND room_id=?",
                (batch_id, room_id),
            ).fetchone()
            if not batch:
                db.rollback()
                return False
            if batch["state"] == "acknowledged":
                db.commit()
                return True
            if batch["state"] != "in_flight":
                db.commit()
                return True
            if not batch["claimed"]:
                db.rollback()
                return False
            if success and not batch["included"]:
                db.rollback()
                return False
            if not success:
                db.execute(
                    "UPDATE peer_batches SET claimed=0,session_key=NULL,turn_key=NULL,claim_owner=NULL,content_sha256=NULL "
                    "WHERE batch_id=?",
                    (batch_id,),
                )
                db.commit()
                return True
            ids = json.loads(batch["event_ids"])
            if ids:
                placeholders = ",".join("?" for _ in ids)
                db.execute(
                    f"UPDATE peer_messages SET delivered=1 WHERE room_id=? AND event_id IN ({placeholders})",
                    (room_id, *ids),
                )
            if not batch["omission_settled"]:
                db.execute(
                    "UPDATE peer_buffer_meta SET delivered_omitted_count=delivered_omitted_count+? WHERE room_id=?",
                    (int(batch["omitted_count"]), room_id),
                )
            db.execute(
                "UPDATE peer_batches SET state='delivered',omission_settled=1 WHERE batch_id=?",
                (batch_id,),
            )
            if auto_clear:
                if ids:
                    placeholders = ",".join("?" for _ in ids)
                    db.execute(
                        f"DELETE FROM peer_messages WHERE room_id=? AND event_id IN ({placeholders})",
                        (room_id, *ids),
                    )
                db.execute("UPDATE peer_batches SET state='acknowledged',ack_count=? WHERE batch_id=?",
                           (len(ids), batch_id))
            db.commit()
        return True

    def bind_batch_content(self, *, room_id: str, batch_id: str, content_sha256: str) -> bool:
        """Bind a claimed batch to the exact augmented user content before attachment."""
        with self._connect() as db:
            updated = db.execute(
                "UPDATE peer_batches SET content_sha256=? "
                "WHERE room_id=? AND batch_id=? AND state='in_flight' AND claimed=1 "
                "AND (content_sha256 IS NULL OR content_sha256=?)",
                (content_sha256, room_id, batch_id, content_sha256),
            ).rowcount
            db.commit()
        return bool(updated)

    def acknowledge_batch(self, *, room_id: str, batch_id: str, session_key: str | None = None) -> int:
        """Acknowledge a known batch owned by the current Matrix room/session."""
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            batch = db.execute(
                "SELECT event_ids,omitted_count,state,claimed,session_key,read_session_key,"
                "ack_count,included,omission_settled FROM peer_batches WHERE batch_id=? AND room_id=?",
                (batch_id, room_id),
            ).fetchone()
            if batch and batch["state"] == "acknowledged":
                db.commit()
                return int(batch["ack_count"])
            if (not batch or batch["state"] not in {"in_flight", "delivered"}
                    or not batch["included"]
                    or not batch["claimed"]
                    or (session_key and session_key not in {batch["session_key"], batch["read_session_key"]})):
                db.rollback()
                raise ValueError("unknown or undelivered Matrix peer batch")
            ids = json.loads(batch["event_ids"])
            if ids:
                placeholders = ",".join("?" for _ in ids)
                count = db.execute(
                    f"SELECT count(*) FROM peer_messages WHERE room_id=? AND event_id IN ({placeholders})",
                    (room_id, *ids),
                ).fetchone()[0]
                db.execute(
                    f"DELETE FROM peer_messages WHERE room_id=? AND event_id IN ({placeholders})",
                    (room_id, *ids),
                )
            else:
                count = 0
            db.execute("UPDATE peer_batches SET state='acknowledged',ack_count=? WHERE batch_id=?",
                       (int(count), batch_id))
            if not batch["omission_settled"]:
                db.execute(
                    "UPDATE peer_buffer_meta SET delivered_omitted_count=delivered_omitted_count+? "
                    "WHERE room_id=?",
                    (int(batch["omitted_count"]), room_id),
                )
                db.execute("UPDATE peer_batches SET omission_settled=1 WHERE batch_id=?", (batch_id,))
            db.commit()
        return int(count)

    def delivered_batches(self, *, room_id: str, limit: int = 20) -> list[dict]:
        """Return bounded metadata for delivered batches still awaiting acknowledgement."""
        bounded = max(1, min(int(limit), 50))
        with self._connect() as db:
            rows = db.execute(
                "SELECT batch_id,event_ids,omitted_count,session_key,turn_key,content_sha256 "
                "FROM peer_batches WHERE room_id=? AND state='delivered' "
                "ORDER BY rowid DESC LIMIT ?",
                (room_id, bounded),
            ).fetchall()
        return [
            {
                "batch_id": row["batch_id"],
                "event_count": len(json.loads(row["event_ids"])),
                "omitted_count": int(row["omitted_count"]),
                "session_id": row["session_key"],
                "inbound_event_id": row["turn_key"],
                "content_sha256": row["content_sha256"] or "",
            }
            for row in rows
        ]

    def read_delivered_batch(
        self, *, room_id: str, batch_id: str, session_key: str,
    ) -> dict | None:
        """Expose one delivered batch and atomically authorize its reader to acknowledge it."""
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            batch = db.execute(
                "SELECT event_ids,omitted_count FROM peer_batches "
                "WHERE room_id=? AND batch_id=? AND state='delivered' AND claimed=1",
                (room_id, batch_id),
            ).fetchone()
            if not batch:
                db.rollback()
                return None
            ids = json.loads(batch["event_ids"])
            messages = []
            if ids:
                placeholders = ",".join("?" for _ in ids)
                messages = [dict(row) for row in db.execute(
                    f"SELECT event_id,sender,timestamp,body FROM peer_messages "
                    f"WHERE room_id=? AND event_id IN ({placeholders}) ORDER BY timestamp,event_id",
                    (room_id, *ids),
                ).fetchall()]
            db.execute(
                "UPDATE peer_batches SET read_session_key=? WHERE room_id=? AND batch_id=? "
                "AND state='delivered'",
                (session_key, room_id, batch_id),
            )
            db.commit()
        return {
            "batch_id": batch_id,
            "messages": messages,
            "omitted_count": int(batch["omitted_count"]),
        }

    def batch_receipt(self, *, room_id: str, batch_id: str) -> dict | None:
        """Return bounded identity needed to prove this batch's exact canonical row."""
        with self._connect() as db:
            row = db.execute(
                "SELECT session_key,turn_key,event_ids FROM peer_batches "
                "WHERE room_id=? AND batch_id=? AND claimed=1",
                (room_id, batch_id),
            ).fetchone()
        if not row or not row["session_key"] or not row["turn_key"]:
            return None
        return {
            "session_id": row["session_key"],
            "inbound_event_id": row["turn_key"],
            "event_ids": json.loads(row["event_ids"]),
            "content_sha256": self._batch_content_hash(room_id=room_id, batch_id=batch_id),
        }

    def _batch_content_hash(self, *, room_id: str, batch_id: str) -> str:
        with self._connect() as db:
            row = db.execute(
                "SELECT content_sha256 FROM peer_batches WHERE room_id=? AND batch_id=?",
                (room_id, batch_id),
            ).fetchone()
        return str(row[0] or "") if row else ""

    def mark_included(self, *, room_id: str, batch_id: str) -> bool:
        """Record the read-back proof that the exact user row committed."""
        with self._connect() as db:
            updated = db.execute(
                "UPDATE peer_batches SET included=1 WHERE room_id=? AND batch_id=? AND state='in_flight' AND claimed=1",
                (room_id, batch_id),
            ).rowcount
            db.commit()
        return bool(updated)

    def acknowledge(self, *, room_id: str, event_ids: Iterable[str]) -> int:
        """Compatibility guard: acknowledgements must use a batch identity."""
        del event_ids
        raise ValueError("batch_id is required for Matrix peer acknowledgements")
