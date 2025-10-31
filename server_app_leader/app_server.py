import grpc
from concurrent import futures
import threading
import uuid
import json
import os
from datetime import datetime, timezone

import app_pb2
import app_pb2_grpc
import llm_pb2
import llm_pb2_grpc
from .auth import generate_token, verify_token


class AppServiceServicer(app_pb2_grpc.AppServiceServicer):
    def __init__(self):
        # Log file for Raft/auditing (create this first)
        self.log_file = "server_app_leader/operation_log.jsonl"
        self._ensure_log_file()
        
        # Log index counter (for Raft term/index tracking)
        self.log_index = 0
        self.log_lock = threading.Lock()

        # Recover state from logs
        self.inventory = {}
        self._recover_from_logs()

        # Global lock for consistent snapshots
        self.inventory_lock = threading.RLock()

        # Per-item lock registry (auto-creates locks for new items)
        self._locks = {}
        self._locks_guard = threading.Lock()

        # Simple user store
        self.users = {"user": "123", "admin": "admin123", "jenil":"1234"}

        # LLM client
        self.llm_channel = grpc.insecure_channel("localhost:50052")
        self.llm_stub = llm_pb2_grpc.LLMServiceStub(self.llm_channel)

    def _ensure_log_file(self):
        os.makedirs(os.path.dirname(self.log_file), exist_ok=True)
        if not os.path.exists(self.log_file):
            with open(self.log_file, 'w') as f:
                pass

    def _recover_from_logs(self):
        """Recover inventory state by replaying all logs"""
        if not os.path.exists(self.log_file):
            print("[Leader] No previous logs found, starting fresh")
            return
        
        recovered_count = 0
        with open(self.log_file, 'r') as f:
            for line in f:
                if line.strip():
                    try:
                        log_entry = json.loads(line)
                        product = log_entry.get("product")
                        operation = log_entry.get("operation")
                        
                        if operation == "add_inventory":
                            self.inventory[product] = self.inventory.get(product, 0) + log_entry["qty_change"]
                        elif operation == "update_inventory":
                            self.inventory[product] = log_entry["new_qty"]
                        
                        # Track highest log index
                        self.log_index = max(self.log_index, log_entry.get("index", 0))
                        recovered_count += 1
                    except Exception as e:
                        print(f"[Leader] Error recovering log entry: {e}")
        
        print(f"[Leader] Recovered {recovered_count} operations from logs")
        print(f"[Leader] Current inventory state: {self.inventory}")
        print(f"[Leader] Last log index: {self.log_index}")

    def _append_log(self, operation: str, product: str, qty: int, username: str, new_qty: int):
        """Append operation to persistent log (for Raft & LLM context)"""
        with self.log_lock:
            self.log_index += 1
            log_entry = {
                "index": self.log_index,
                "timestamp": datetime.now(timezone.utc).isoformat(),  # ✓ Use timezone.utc
                "operation": operation,
                "product": product,
                "qty_change": qty,
                "new_qty": new_qty,
                "username": username,
                "term": 1  # For future Raft implementation
            }
            with open(self.log_file, 'a') as f:
                f.write(json.dumps(log_entry) + '\n')
            return log_entry

    def _get_item_lock(self, product: str) -> threading.Lock:
        with self._locks_guard:
            lock = self._locks.get(product)
            if lock is None:
                lock = threading.Lock()
                self._locks[product] = lock
            return lock

    def login(self, request, context):
        if request.username not in self.users or self.users[request.username] != request.password:
            return app_pb2.LoginResponse(status="unauthorized", token="")
        token = generate_token(request.username)
        print(f"[Leader] User '{request.username}' logged in")
        return app_pb2.LoginResponse(status="success", token=token)

    def _verify_auth(self, token: str, context):
        try:
            return verify_token(token)
        except ValueError as e:
            context.set_code(grpc.StatusCode.UNAUTHENTICATED)
            context.set_details(str(e))
            return None

    def post(self, request, context):
        username = self._verify_auth(request.token, context)
        if not username:
            return app_pb2.PostResponse(status="unauthorized", result="")

        if request.type == "add_inventory":
            try:
                product, qty_s = request.data.split(":", 1)
                product = product.strip()
                qty = int(qty_s)
                if not product:
                    raise ValueError("empty product")
            except Exception:
                return app_pb2.PostResponse(status="bad_request", result="Format must be Product:Qty")

            item_lock = self._get_item_lock(product)
            if not item_lock.acquire(timeout=5.0):
                context.set_code(grpc.StatusCode.ABORTED)
                context.set_details("Resource busy, try again")
                return app_pb2.PostResponse(status="busy", result="")

            try:
                with self.inventory_lock:
                    old_qty = self.inventory.get(product, 0)
                    self.inventory[product] = old_qty + qty
                    new_qty = self.inventory[product]
                
                # Log the operation
                log_entry = self._append_log("add_inventory", product, qty, username, new_qty)
                print(f"[Leader] Log #{log_entry['index']}: {username} added {qty} units of {product}, new total: {new_qty}")
                
                # Send only this update to LLM server
                self._send_update_to_llm(log_entry)
                
                return app_pb2.PostResponse(status="added", result=f"{product}:{new_qty}")
            finally:
                item_lock.release()

        elif request.type == "update_inventory":
            # Update: set absolute quantity (Format: "Product:NewQty")
            try:
                product, qty_s = request.data.split(":", 1)
                product = product.strip()
                new_qty = int(qty_s)
                if not product:
                    raise ValueError("empty product")
                if new_qty < 0:
                    raise ValueError("quantity cannot be negative")
            except Exception as e:
                return app_pb2.PostResponse(status="bad_request", result=f"Format must be Product:NewQty, error: {str(e)}")

            item_lock = self._get_item_lock(product)
            if not item_lock.acquire(timeout=5.0):
                context.set_code(grpc.StatusCode.ABORTED)
                context.set_details("Resource busy, try again")
                return app_pb2.PostResponse(status="busy", result="")

            try:
                with self.inventory_lock:
                    if product not in self.inventory:
                        item_lock.release()
                        return app_pb2.PostResponse(status="not_found", result=f"Product '{product}' does not exist")
                    
                    old_qty = self.inventory[product]
                    self.inventory[product] = new_qty
                    qty_change = new_qty - old_qty
                
                # Log the operation
                log_entry = self._append_log("update_inventory", product, qty_change, username, new_qty)
                print(f"[Leader] Log #{log_entry['index']}: {username} updated {product} from {old_qty} to {new_qty} (change: {qty_change:+d})")
                
                # Send update to LLM server
                self._send_update_to_llm(log_entry)
                
                return app_pb2.PostResponse(status="updated", result=f"{product}:{new_qty} (was {old_qty})")
            finally:
                item_lock.release()

        elif request.type == "query_ai":
            # Snapshot current state for AI context
            with self.inventory_lock:
                ctx = ";".join(f"{k}:{v}" for k, v in self.inventory.items())
            
            llm_resp = self.llm_stub.getLLMAnswer(llm_pb2.LLMRequest(
                requestId=str(uuid.uuid4()),
                query=request.data,
                context=ctx
            ))
            return app_pb2.PostResponse(status="ok", result=llm_resp.answer)

        return app_pb2.PostResponse(status="unknown_type", result="")

    def _send_update_to_llm(self, log_entry: dict):
        """Send incremental update to LLM server"""
        try:
            update_msg = f"{log_entry['operation']}:{log_entry['product']}:{log_entry['qty_change']}:{log_entry['new_qty']}"
            # Use context field to send the update
            self.llm_stub.getLLMAnswer(llm_pb2.LLMRequest(
                requestId=str(uuid.uuid4()),
                query="__LOG_UPDATE__",  # Special marker
                context=update_msg
            ))
            print(f"[Leader] Sent log update to LLM server: {update_msg}")
        except Exception as e:
            print(f"[Leader] Failed to send update to LLM: {e}")

    def get(self, request, context):
        username = self._verify_auth(request.token, context)
        if not username:
            return app_pb2.GetResponse(status="unauthorized", items=[])

        with self.inventory_lock:
            items = [f"{k}:{v}" for k, v in self.inventory.items()]
        return app_pb2.GetResponse(status="ok", items=items)

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    app_pb2_grpc.add_AppServiceServicer_to_server(AppServiceServicer(), server)
    server.add_insecure_port("[::]:50051")
    print("App Server running on port 50051...")
    server.start()
    server.wait_for_termination()

if __name__ == "__main__":
    serve()
