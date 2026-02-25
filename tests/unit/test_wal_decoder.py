"""Unit tests for pgoutput binary protocol decoder."""

from __future__ import annotations

import struct

from cdc_platform.sources.wal.decoder import PgOutputDecoder


def _build_begin(lsn: int = 100, ts_us: int = 0, xid: int = 1) -> bytes:
    """Build a Begin message: 'B' + lsn(8) + timestamp(8) + xid(4)."""
    return (
        b"B"
        + struct.pack("!Q", lsn)
        + struct.pack("!q", ts_us)
        + struct.pack("!I", xid)
    )


def _build_relation(
    rel_id: int = 1,
    namespace: str = "public",
    table: str = "users",
    columns: list[tuple[str, int]] | None = None,
) -> bytes:
    """Build a Relation message."""
    if columns is None:
        columns = [("id", 23), ("name", 25)]  # int4, text

    data = b"R"
    data += struct.pack("!I", rel_id)
    data += namespace.encode() + b"\x00"
    data += table.encode() + b"\x00"
    data += b"\x00"  # replica identity
    data += struct.pack("!H", len(columns))

    for col_name, type_oid in columns:
        data += b"\x00"  # flags
        data += col_name.encode() + b"\x00"
        data += struct.pack("!I", type_oid)
        data += struct.pack("!I", 0)  # type modifier

    return data


def _build_tuple_data(values: list[str | None]) -> bytes:
    """Build TupleData for Insert/Update/Delete messages."""
    data = struct.pack("!H", len(values))
    for val in values:
        if val is None:
            data += b"n"
        else:
            encoded = val.encode("utf-8")
            data += b"t" + struct.pack("!I", len(encoded)) + encoded
    return data


def _build_insert(rel_id: int = 1, values: list[str | None] | None = None) -> bytes:
    """Build an Insert message: 'I' + rel_id(4) + 'N' + TupleData."""
    if values is None:
        values = ["1", "Alice"]
    return b"I" + struct.pack("!I", rel_id) + b"N" + _build_tuple_data(values)


def _build_delete(rel_id: int = 1, values: list[str | None] | None = None) -> bytes:
    """Build a Delete message: 'D' + rel_id(4) + 'K' + TupleData."""
    if values is None:
        values = ["1", "Alice"]
    return b"D" + struct.pack("!I", rel_id) + b"K" + _build_tuple_data(values)


def _build_update(
    rel_id: int = 1,
    new_values: list[str | None] | None = None,
) -> bytes:
    """Build an Update message (no old tuple): 'U' + rel_id(4) + 'N' + TupleData."""
    if new_values is None:
        new_values = ["1", "Bob"]
    return b"U" + struct.pack("!I", rel_id) + b"N" + _build_tuple_data(new_values)


class TestPgOutputDecoder:
    def test_begin_sets_lsn_and_timestamp(self):
        decoder = PgOutputDecoder()
        result = decoder.decode(_build_begin(lsn=42, ts_us=1_000_000))
        assert result == []
        assert decoder._current_lsn == 42

    def test_commit_returns_empty(self):
        decoder = PgOutputDecoder()
        # Commit: 'C' + flags(1) + lsn(8) + end_lsn(8) + timestamp(8)
        commit_data = b"C" + b"\x00" + struct.pack("!Q", 0) * 2 + struct.pack("!q", 0)
        result = decoder.decode(commit_data)
        assert result == []

    def test_relation_cached(self):
        decoder = PgOutputDecoder()
        decoder.decode(_build_relation(rel_id=5, namespace="myschema", table="orders"))
        assert 5 in decoder._relations
        assert decoder._relations[5].schema == "myschema"
        assert decoder._relations[5].table == "orders"

    def test_insert_decoded(self):
        decoder = PgOutputDecoder()
        decoder.decode(_build_begin())
        decoder.decode(_build_relation())
        changes = decoder.decode(_build_insert(values=["42", "Alice"]))

        assert len(changes) == 1
        change = changes[0]
        assert change.operation == "insert"
        assert change.schema == "public"
        assert change.table == "users"
        assert change.after == {"id": "42", "name": "Alice"}
        assert change.before is None

    def test_delete_decoded(self):
        decoder = PgOutputDecoder()
        decoder.decode(_build_begin())
        decoder.decode(_build_relation())
        changes = decoder.decode(_build_delete(values=["42", "Alice"]))

        assert len(changes) == 1
        change = changes[0]
        assert change.operation == "delete"
        assert change.before == {"id": "42", "name": "Alice"}
        assert change.after is None

    def test_update_decoded(self):
        decoder = PgOutputDecoder()
        decoder.decode(_build_begin())
        decoder.decode(_build_relation())
        changes = decoder.decode(_build_update(new_values=["42", "Bob"]))

        assert len(changes) == 1
        change = changes[0]
        assert change.operation == "update"
        assert change.after == {"id": "42", "name": "Bob"}

    def test_null_values(self):
        decoder = PgOutputDecoder()
        decoder.decode(_build_begin())
        decoder.decode(_build_relation())
        changes = decoder.decode(_build_insert(values=["1", None]))

        assert changes[0].after == {"id": "1", "name": None}

    def test_empty_data(self):
        decoder = PgOutputDecoder()
        assert decoder.decode(b"") == []

    def test_unknown_message_type(self):
        decoder = PgOutputDecoder()
        assert decoder.decode(b"X" + b"\x00" * 10) == []

    def test_multiple_tables(self):
        decoder = PgOutputDecoder()
        decoder.decode(_build_begin())
        decoder.decode(_build_relation(rel_id=1, namespace="public", table="users"))
        decoder.decode(
            _build_relation(
                rel_id=2,
                namespace="public",
                table="orders",
                columns=[("order_id", 23), ("amount", 1700)],
            )
        )

        changes1 = decoder.decode(_build_insert(rel_id=1, values=["1", "Alice"]))
        changes2 = decoder.decode(_build_insert(rel_id=2, values=["100", "99.99"]))

        assert changes1[0].table == "users"
        assert changes2[0].table == "orders"
        assert changes2[0].after == {"order_id": "100", "amount": "99.99"}
