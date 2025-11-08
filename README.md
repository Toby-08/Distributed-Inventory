# 🏬 Distributed Inventory Management System

## 🔧 Environment Setup (for all members)

### Clone the repo
```Git bash
git clone https://github.com/Toby-08/Distributed-Inventory.git
cd distributed-inventory
python -m venv venv
source venv/Scripts/activate
pip install -r requirements.txt
```

## 🚀 Current Features

- Leader-based gRPC services
  - App Server (port 50051) for inventory and auth
  - LLM Server (port 50052) for AI responses and log ingestion
- Authentication
  - JWT-based login (1h expiry) via server_app_leader/auth.py
- Inventory operations
  - Add inventory: increase quantity (creates product if missing)
  - Update inventory: set absolute quantity for an existing product
  - Get inventory: consistent snapshot of all items
- Consistency (single server instance)
  - Per-item locks prevent concurrent writes on the same product
  - Global snapshot lock ensures consistent reads
  - Busy requests return gRPC ABORTED; clients can retry
- Persistent, Raft-ready logging
  - Append-only operation_log.jsonl with index, timestamp, operation, product, qty_change, new_qty, username, term
  - State recovery on startup by replaying logs (inventory rebuilt; index continues)
- LLM integration with incremental updates
  - Leader sends only new updates to LLM using a special message (query="__LOG_UPDATE__")
  - LLM server persists llm_context_log.jsonl, loads history on start, and uses both history + current inventory context in answers
- Protobuf and tooling
  - app.proto and llm.proto compiled via grpcio-tools
  - Generated files: *_pb2.py, *_pb2_grpc.py
- Packaging and execution
  - Packages: server_app_leader, llm_server, client (with __init__.py)
  - Run with python -m to preserve package imports
- Minimal dependencies
  - grpcio, grpcio-tools, protobuf, PyJWT
- Git hygiene
  - .gitignore excludes logs (*.jsonl), __pycache__, venv

## ▶️ Run

- Terminal 1:
  - python -m llm_server.llm_server
- Terminal 2:
  - python -m server_app_leader.app_server
- Terminal 3 (multi-clients supported):
  - python -m client.client
  
---

## 📸 Working Screenshot

View screenshots:
- https://drive.google.com/drive/folders/1qAN__CROhQ8pYbRRgvokdfNHImSeI_H-?usp=sharing

---

## 📌 Notes

- Active branches:
  - feature-grpc-auth
  - feature-raft-logic
  - feature-llm
- Current sprint: Raft
  - Route new updates to: feature-raft-logic

---
