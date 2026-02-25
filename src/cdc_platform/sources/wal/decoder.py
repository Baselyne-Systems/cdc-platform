"""pgoutput binary protocol decoder.

Decodes the pgoutput logical replication plugin wire format into
structured WalChange dataclasses.  Handles message types:

- B (Begin)          — transaction start
- C (Commit)         — transaction commit
- R (Relation)       — table schema definition
- I (Insert)         — row insert
- U (Update)         — row update
- D (Delete)         — row delete

Reference: https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html
"""

from __future__ import annotations

import struct
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Literal

# PostgreSQL epoch: 2000-01-01 00:00:00 UTC
_PG_EPOCH = datetime(2000, 1, 1, tzinfo=UTC)
_PG_EPOCH_TS = _PG_EPOCH.timestamp()


@dataclass(slots=True)
class WalChange:
    """A single decoded WAL change event."""

    operation: Literal["insert", "update", "delete"]
    schema: str
    table: str
    before: dict[str, Any] | None
    after: dict[str, Any] | None
    lsn: int
    timestamp: datetime


@dataclass
class _RelationInfo:
    """Cached relation (table) metadata from Relation messages."""

    schema: str
    table: str
    columns: list[tuple[str, int]]  # (name, type_oid)


class PgOutputDecoder:
    """Stateful decoder for the pgoutput binary wire protocol.

    Maintains a relation cache so that Insert/Update/Delete messages
    can resolve column names from the most recent Relation message.
    """

    def __init__(self) -> None:
        self._relations: dict[int, _RelationInfo] = {}
        self._current_lsn: int = 0
        self._current_timestamp: datetime = _PG_EPOCH

    def decode(self, data: bytes) -> list[WalChange]:
        """Decode a pgoutput message, returning zero or more WalChange events.

        Relation/Begin/Commit messages return an empty list (metadata only).
        Insert/Update/Delete messages return a single WalChange.
        """
        if not data:
            return []

        msg_type = chr(data[0])
        payload = data[1:]

        if msg_type == "B":
            self._decode_begin(payload)
            return []
        elif msg_type == "C":
            return []
        elif msg_type == "R":
            self._decode_relation(payload)
            return []
        elif msg_type == "I":
            return [self._decode_insert(payload)]
        elif msg_type == "U":
            return [self._decode_update(payload)]
        elif msg_type == "D":
            return [self._decode_delete(payload)]
        else:
            return []

    def _decode_begin(self, data: bytes) -> None:
        """Parse Begin message: final LSN (8) + commit timestamp (8) + xid (4)."""
        lsn = struct.unpack_from("!Q", data, 0)[0]
        ts_us = struct.unpack_from("!q", data, 8)[0]
        self._current_lsn = lsn
        self._current_timestamp = datetime.fromtimestamp(
            _PG_EPOCH_TS + ts_us / 1_000_000, tz=UTC
        )

    def _decode_relation(self, data: bytes) -> None:
        """Parse Relation message: rel_id + namespace + name + columns."""
        offset = 0
        rel_id = struct.unpack_from("!I", data, offset)[0]
        offset += 4

        namespace, offset = self._read_string(data, offset)
        table, offset = self._read_string(data, offset)

        # replica identity (1 byte)
        offset += 1

        n_cols = struct.unpack_from("!H", data, offset)[0]
        offset += 2

        columns: list[tuple[str, int]] = []
        for _ in range(n_cols):
            # flags (1 byte)
            offset += 1
            col_name, offset = self._read_string(data, offset)
            type_oid = struct.unpack_from("!I", data, offset)[0]
            offset += 4
            # type modifier (4 bytes)
            offset += 4
            columns.append((col_name, type_oid))

        self._relations[rel_id] = _RelationInfo(
            schema=namespace, table=table, columns=columns
        )

    def _decode_insert(self, data: bytes) -> WalChange:
        """Parse Insert message: rel_id (4) + 'N' + TupleData."""
        rel_id = struct.unpack_from("!I", data, 0)[0]
        rel = self._relations[rel_id]
        # Skip rel_id (4) + 'N' marker (1)
        row = self._decode_tuple_data(data, 5, rel.columns)
        return WalChange(
            operation="insert",
            schema=rel.schema,
            table=rel.table,
            before=None,
            after=row,
            lsn=self._current_lsn,
            timestamp=self._current_timestamp,
        )

    def _decode_update(self, data: bytes) -> WalChange:
        """Parse Update message: rel_id (4) + optional old tuple + 'N' + new tuple."""
        rel_id = struct.unpack_from("!I", data, 0)[0]
        rel = self._relations[rel_id]
        offset = 4

        before = None
        marker = chr(data[offset])

        # Old tuple present if marker is 'K' (key) or 'O' (old)
        if marker in ("K", "O"):
            offset += 1
            before, offset = self._decode_tuple_data_with_offset(
                data, offset, rel.columns
            )
            # Next byte should be 'N' for new tuple
            offset += 1  # skip 'N'
        else:
            offset += 1  # skip 'N'

        after = self._decode_tuple_data(data, offset, rel.columns)

        return WalChange(
            operation="update",
            schema=rel.schema,
            table=rel.table,
            before=before,
            after=after,
            lsn=self._current_lsn,
            timestamp=self._current_timestamp,
        )

    def _decode_delete(self, data: bytes) -> WalChange:
        """Parse Delete message: rel_id (4) + 'K'|'O' + TupleData."""
        rel_id = struct.unpack_from("!I", data, 0)[0]
        rel = self._relations[rel_id]
        # Skip rel_id (4) + key/old marker (1)
        before = self._decode_tuple_data(data, 5, rel.columns)
        return WalChange(
            operation="delete",
            schema=rel.schema,
            table=rel.table,
            before=before,
            after=None,
            lsn=self._current_lsn,
            timestamp=self._current_timestamp,
        )

    def _decode_tuple_data(
        self, data: bytes, start: int, columns: list[tuple[str, int]]
    ) -> dict[str, Any]:
        """Decode TupleData starting at *start*, return column dict."""
        result, _ = self._decode_tuple_data_with_offset(data, start, columns)
        return result

    def _decode_tuple_data_with_offset(
        self, data: bytes, start: int, columns: list[tuple[str, int]]
    ) -> tuple[dict[str, Any], int]:
        """Decode TupleData, returning (dict, new_offset)."""
        offset = start
        n_cols = struct.unpack_from("!H", data, offset)[0]
        offset += 2

        row: dict[str, Any] = {}
        for i in range(n_cols):
            col_type = chr(data[offset])
            offset += 1

            col_name = columns[i][0] if i < len(columns) else f"col_{i}"

            if col_type == "n":
                # NULL
                row[col_name] = None
            elif col_type == "u":
                # Unchanged TOASTed value
                row[col_name] = None
            elif col_type == "t":
                # Text value
                val_len = struct.unpack_from("!I", data, offset)[0]
                offset += 4
                val_bytes = data[offset : offset + val_len]
                row[col_name] = val_bytes.decode("utf-8")
                offset += val_len
            else:
                row[col_name] = None

        return row, offset

    @staticmethod
    def _read_string(data: bytes, offset: int) -> tuple[str, int]:
        """Read a null-terminated string from *data* at *offset*."""
        end = data.index(0, offset)
        s = data[offset:end].decode("utf-8")
        return s, end + 1
