#!/usr/bin/env bash
# Initialise a single-node MongoDB replica set and seed the cdc_demo database.
# Runs as a one-shot container after the mongodb service is healthy.
set -euo pipefail

HOST="${MONGO_HOST:-mongodb}"
PORT="${MONGO_PORT:-27017}"
ENDPOINT="${HOST}:${PORT}"

echo "[mongodb-setup] Initialising replica set on ${ENDPOINT}..."
mongosh --host "${ENDPOINT}" --quiet --eval '
  const status = rs.status();
  if (status.ok === 1 && status.set !== undefined) {
    print("[mongodb-setup] Replica set already initialised — skipping.");
  } else {
    print("[mongodb-setup] Running rs.initiate()...");
    const result = rs.initiate({
      _id: "rs0",
      members: [{ _id: 0, host: "mongodb:27017" }]
    });
    if (result.ok !== 1) {
      printjson(result);
      quit(1);
    }
    print("[mongodb-setup] rs.initiate() succeeded.");
  }
'

echo "[mongodb-setup] Waiting 6 s for PRIMARY election..."
sleep 6

echo "[mongodb-setup] Seeding cdc_demo database..."
mongosh \
  "mongodb://${ENDPOINT}/cdc_demo?replicaSet=rs0" \
  --quiet \
  --eval '
    if (db.products.countDocuments() === 0) {
      db.products.insertMany([
        { _id: 1, name: "Widget A", price: 9.99,  stock: 100, category: "widgets" },
        { _id: 2, name: "Widget B", price: 19.99, stock: 50,  category: "gadgets" }
      ]);
      print("[mongodb-setup] Seeded products.");
    }
    if (db.orders.countDocuments() === 0) {
      db.orders.insertMany([
        { _id: 1, product_id: 1, quantity: 2, status: "pending"   },
        { _id: 2, product_id: 2, quantity: 1, status: "completed" }
      ]);
      print("[mongodb-setup] Seeded orders.");
    }
  '

echo "[mongodb-setup] Done."
