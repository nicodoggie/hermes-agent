"""Durable, profile-scoped buffering for Matrix peer messages."""
from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


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
                db.executescript("""
                    PRAGMA journal_mode=WAL;
                    CREATE TABLE IF NOT EXISTS peer_messages (
                        room_id TEXT NOT NULL,
                        event_id TEXT NOT NULL,
                        sender TEXT NOT NULL,
                        timestamp REAL NOT NULL,
                        body TEXT NOT NULL,
                        body_bytes INTEGER NOT NULL,
                        PRIMARY KEY (room_id, event_id)
                    );
                    CREATE TABLE IF NOT EXISTS peer_buffer_meta (
                        room_id TEXT PRIMARY KEY,
                        omitted_count INTEGER NOT NULL DEFAULT 0
                    );
                    CREATE TABLE IF NOT EXISTS peer_message_acks (
                        room_id TEXT NOT NULL,
                        event_id TEXT NOT NULL,
                        PRIMARY KEY (room_id, event_id)
                    );
                """)

    def _connect(self) -> sqlite3.Connection:
        db = sqlite3.connect(self.db_path, timeout=10)
        db.row_factory = sqlite3.Row
        return db

    def add(self, *, room_id: str, event_id: str, sender: str, timestamp: float, body: str) -> bool:
        if not self.config.enabled or not room_id or not event_id:
            return False
        raw = str(body)
        encoded = raw.encode("utf-8")
        cap = self.config.max_message_bytes
        if len(encoded) > cap:
            encoded = encoded[:cap]
            while True:
                try:
                    body = encoded.decode("utf-8")
                    break
                except UnicodeDecodeError:
                    encoded = encoded[:-1]
            body += "… [message truncated]"
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
                db.execute("UPDATE peer_buffer_meta SET omitted_count=omitted_count+? WHERE room_id=?", (evicted, room_id))
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
            meta = db.execute("SELECT omitted_count FROM peer_buffer_meta WHERE room_id=?", (room_id,)).fetchone()
        return {"messages": [dict(row) for row in rows], "omitted_count": int(meta[0]) if meta else 0}

    def acknowledge(self, *, room_id: str, event_ids: Iterable[str]) -> int:
        if not self.config.enabled or not room_id:
            return 0
        ids = list(dict.fromkeys(str(value) for value in event_ids if str(value)))
        if not ids:
            return 0
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            placeholders = ",".join("?" for _ in ids)
            foreign = db.execute(
                f"SELECT event_id FROM peer_messages WHERE event_id IN ({placeholders}) AND room_id != ?",
                (*ids, room_id),
            ).fetchone()
            if foreign:
                raise ValueError("event_ids are scoped to another Matrix room")
            count = db.execute(
                f"SELECT count(*) FROM peer_messages WHERE room_id=? AND event_id IN ({placeholders})",
                (room_id, *ids),
            ).fetchone()[0]
            db.executemany(
                "INSERT OR IGNORE INTO peer_message_acks(room_id,event_id) VALUES (?,?)",
                ((room_id, event_id) for event_id in ids),
            )
            db.execute(f"DELETE FROM peer_messages WHERE room_id=? AND event_id IN ({placeholders})", (room_id, *ids))
            db.commit()
        return int(count)
