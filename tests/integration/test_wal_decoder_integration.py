"""Integration tests for pgoutput WAL decoder — realistic multi-table scenarios.

These tests verify that the decoder handles complete transaction sequences,
multiple interleaved tables, large column counts, and edge cases that only
arise when processing realistic WAL streams.
"""

from __future__ import annotations

import struct

import pytest

from cdc_platform.sources.wal.decoder import PgOutputDecoder

# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _begin(lsn: int = 100, ts_us: int = 1_000_000, xid: int = 1) -> bytes:
    return (
        b"B"
        + struct.pack("!Q", lsn)
        + struct.pack("!q", ts_us)
        + struct.pack("!I", xid)
    )


def _commit(lsn: int = 100) -> bytes:
    return (
        b"C"
        + b"\x00"
        + struct.pack("!Q", lsn)
        + struct.pack("!Q", lsn)
        + struct.pack("!q", 0)
    )


def _relation(
    rel_id: int,
    namespace: str,
    table: str,
    columns: list[tuple[str, int]],
) -> bytes:
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


def _tuple_data(values: list[str | None]) -> bytes:
    data = struct.pack("!H", len(values))
    for val in values:
        if val is None:
            data += b"n"
        else:
            encoded = val.encode("utf-8")
            data += b"t" + struct.pack("!I", len(encoded)) + encoded
    return data


def _insert(rel_id: int, values: list[str | None]) -> bytes:
    return b"I" + struct.pack("!I", rel_id) + b"N" + _tuple_data(values)


def _update_no_old(rel_id: int, new_values: list[str | None]) -> bytes:
    return b"U" + struct.pack("!I", rel_id) + b"N" + _tuple_data(new_values)


def _update_with_old(
    rel_id: int, old_values: list[str | None], new_values: list[str | None]
) -> bytes:
    return (
        b"U"
        + struct.pack("!I", rel_id)
        + b"O"
        + _tuple_data(old_values)
        + b"N"
        + _tuple_data(new_values)
    )


def _delete(rel_id: int, values: list[str | None]) -> bytes:
    return b"D" + struct.pack("!I", rel_id) + b"K" + _tuple_data(values)


# ---------------------------------------------------------------------------
# tests
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestWalDecoderMultiTable:
    """Verify correct decoding of interleaved multi-table WAL streams."""

    def test_interleaved_inserts_across_tables(self):
        """Multiple tables in a single transaction should each decode correctly."""
        decoder = PgOutputDecoder()

        # Transaction with inserts to two different tables
        decoder.decode(_begin(lsn=1000))
        decoder.decode(_relation(1, "public", "users", [("id", 23), ("email", 25)]))
        decoder.decode(
            _relation(
                2, "public", "orders", [("id", 23), ("user_id", 23), ("total", 1700)]
            )
        )

        c1 = decoder.decode(_insert(1, ["1", "alice@example.com"]))
        c2 = decoder.decode(_insert(2, ["100", "1", "49.99"]))
        c3 = decoder.decode(_insert(1, ["2", "bob@example.com"]))
        c4 = decoder.decode(_insert(2, ["101", "2", "99.99"]))
        decoder.decode(_commit())

        assert len(c1) == 1
        assert c1[0].table == "users"
        assert c1[0].after == {"id": "1", "email": "alice@example.com"}

        assert len(c2) == 1
        assert c2[0].table == "orders"
        assert c2[0].after == {"id": "100", "user_id": "1", "total": "49.99"}

        assert c3[0].after["email"] == "bob@example.com"
        assert c4[0].after["total"] == "99.99"

    def test_full_crud_lifecycle(self):
        """INSERT → UPDATE → DELETE on same row should all decode correctly."""
        decoder = PgOutputDecoder()

        decoder.decode(_begin(lsn=2000))
        decoder.decode(
            _relation(
                1, "public", "products", [("id", 23), ("name", 25), ("price", 1700)]
            )
        )

        # INSERT
        inserts = decoder.decode(_insert(1, ["1", "Widget", "9.99"]))
        assert inserts[0].operation == "insert"
        assert inserts[0].after == {"id": "1", "name": "Widget", "price": "9.99"}

        decoder.decode(_commit())

        # UPDATE in new txn
        decoder.decode(_begin(lsn=2001))
        updates = decoder.decode(_update_no_old(1, ["1", "Widget Pro", "19.99"]))
        assert updates[0].operation == "update"
        assert updates[0].after["name"] == "Widget Pro"
        assert updates[0].after["price"] == "19.99"
        decoder.decode(_commit())

        # DELETE in new txn
        decoder.decode(_begin(lsn=2002))
        deletes = decoder.decode(_delete(1, ["1", "Widget Pro", "19.99"]))
        assert deletes[0].operation == "delete"
        assert deletes[0].before["id"] == "1"
        assert deletes[0].after is None
        decoder.decode(_commit())

    def test_update_with_old_tuple(self):
        """UPDATE with REPLICA IDENTITY FULL should decode before/after."""
        decoder = PgOutputDecoder()

        decoder.decode(_begin(lsn=3000))
        decoder.decode(_relation(1, "inventory", "stock", [("sku", 25), ("qty", 23)]))

        changes = decoder.decode(
            _update_with_old(1, ["SKU-001", "100"], ["SKU-001", "95"])
        )

        assert len(changes) == 1
        assert changes[0].operation == "update"
        assert changes[0].before == {"sku": "SKU-001", "qty": "100"}
        assert changes[0].after == {"sku": "SKU-001", "qty": "95"}
        assert changes[0].schema == "inventory"

    def test_many_columns(self):
        """Tables with many columns should decode without truncation."""
        decoder = PgOutputDecoder()
        col_count = 50
        columns = [(f"col_{i}", 25) for i in range(col_count)]

        decoder.decode(_begin())
        decoder.decode(_relation(1, "wide", "big_table", columns))

        values = [f"val_{i}" for i in range(col_count)]
        changes = decoder.decode(_insert(1, values))

        assert len(changes) == 1
        assert len(changes[0].after) == col_count
        for i in range(col_count):
            assert changes[0].after[f"col_{i}"] == f"val_{i}"

    def test_unicode_values(self):
        """Non-ASCII values should decode correctly."""
        decoder = PgOutputDecoder()

        decoder.decode(_begin())
        decoder.decode(_relation(1, "public", "i18n", [("id", 23), ("name", 25)]))

        changes = decoder.decode(_insert(1, ["1", "日本語テスト"]))
        assert changes[0].after["name"] == "日本語テスト"

    def test_empty_string_values(self):
        """Empty strings should decode as empty strings, not None."""
        decoder = PgOutputDecoder()

        decoder.decode(_begin())
        decoder.decode(_relation(1, "public", "t", [("id", 23), ("val", 25)]))

        changes = decoder.decode(_insert(1, ["1", ""]))
        assert changes[0].after["val"] == ""

    def test_large_text_values(self):
        """Large text values (simulating TOAST) should decode correctly."""
        decoder = PgOutputDecoder()

        decoder.decode(_begin())
        decoder.decode(_relation(1, "public", "docs", [("id", 23), ("body", 25)]))

        large_text = "x" * 100_000
        changes = decoder.decode(_insert(1, ["1", large_text]))
        assert len(changes[0].after["body"]) == 100_000

    def test_multiple_transactions_preserve_lsn(self):
        """Each transaction should update the LSN tracked by the decoder."""
        decoder = PgOutputDecoder()

        decoder.decode(_relation(1, "public", "t", [("id", 23)]))

        decoder.decode(_begin(lsn=1000))
        c1 = decoder.decode(_insert(1, ["1"]))
        decoder.decode(_commit(lsn=1000))

        decoder.decode(_begin(lsn=2000))
        c2 = decoder.decode(_insert(1, ["2"]))
        decoder.decode(_commit(lsn=2000))

        assert c1[0].lsn == 1000
        assert c2[0].lsn == 2000

    def test_relation_redefinition(self):
        """Schema evolution: same rel_id with new columns after ALTER TABLE."""
        decoder = PgOutputDecoder()

        # v1 schema
        decoder.decode(_begin())
        decoder.decode(_relation(1, "public", "evolving", [("id", 23), ("name", 25)]))
        c1 = decoder.decode(_insert(1, ["1", "Alice"]))
        decoder.decode(_commit())

        # v2 schema — same rel_id, added column
        decoder.decode(_begin())
        decoder.decode(
            _relation(1, "public", "evolving", [("id", 23), ("name", 25), ("age", 23)])
        )
        c2 = decoder.decode(_insert(1, ["2", "Bob", "30"]))
        decoder.decode(_commit())

        assert len(c1[0].after) == 2
        assert len(c2[0].after) == 3
        assert c2[0].after["age"] == "30"

    def test_batch_of_inserts(self):
        """Decode a large batch of inserts in one transaction."""
        decoder = PgOutputDecoder()
        batch_size = 1000

        decoder.decode(_begin(lsn=5000))
        decoder.decode(_relation(1, "public", "bulk", [("id", 23), ("data", 25)]))

        all_changes = []
        for i in range(batch_size):
            changes = decoder.decode(_insert(1, [str(i), f"row_{i}"]))
            all_changes.extend(changes)

        decoder.decode(_commit())

        assert len(all_changes) == batch_size
        assert all_changes[0].after["id"] == "0"
        assert all_changes[-1].after["id"] == str(batch_size - 1)


@pytest.mark.integration
class TestWalDecoderEdgeCases:
    """Edge cases and error resilience."""

    def test_null_in_every_column(self):
        """All-NULL row should decode with all None values."""
        decoder = PgOutputDecoder()

        decoder.decode(_begin())
        decoder.decode(
            _relation(1, "public", "nullable", [("a", 25), ("b", 25), ("c", 25)])
        )

        changes = decoder.decode(_insert(1, [None, None, None]))
        assert changes[0].after == {"a": None, "b": None, "c": None}

    def test_mixed_null_and_values(self):
        """Mix of NULL and non-NULL values."""
        decoder = PgOutputDecoder()

        decoder.decode(_begin())
        decoder.decode(
            _relation(1, "public", "mixed", [("id", 23), ("name", 25), ("bio", 25)])
        )

        changes = decoder.decode(_insert(1, ["1", None, "has a bio"]))
        assert changes[0].after == {"id": "1", "name": None, "bio": "has a bio"}

    def test_unknown_message_types_ignored(self):
        """Unknown pgoutput message types should be silently ignored."""
        decoder = PgOutputDecoder()

        # Types 'T' (truncate), 'O' (origin), 'M' (message) etc.
        assert decoder.decode(b"T\x00\x00\x00\x00") == []
        assert decoder.decode(b"O\x00\x00\x00\x00") == []
        assert decoder.decode(b"M\x00\x00\x00\x00") == []
        assert decoder.decode(b"\xff\x00\x00\x00\x00") == []

    def test_rapid_relation_switches(self):
        """Rapidly switching between relation IDs should use correct schema."""
        decoder = PgOutputDecoder()

        decoder.decode(_begin())
        decoder.decode(_relation(1, "s1", "t1", [("a", 25)]))
        decoder.decode(_relation(2, "s2", "t2", [("b", 25)]))
        decoder.decode(_relation(3, "s3", "t3", [("c", 25)]))

        c1 = decoder.decode(_insert(3, ["val_c"]))
        c2 = decoder.decode(_insert(1, ["val_a"]))
        c3 = decoder.decode(_insert(2, ["val_b"]))

        assert c1[0].schema == "s3" and c1[0].after == {"c": "val_c"}
        assert c2[0].schema == "s1" and c2[0].after == {"a": "val_a"}
        assert c3[0].schema == "s2" and c3[0].after == {"b": "val_b"}
