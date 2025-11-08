import grpc
from concurrent import futures
import sys
import time

import app_pb2
import app_pb2_grpc
import raft_pb2
import raft_pb2_grpc

from server_app_leader.raft_node import RaftNode
from server_app_leader.raft_servicer import RaftServicer
from server_app_leader.auth import authenticate_user, generate_token, verify_token

class AppServiceWithRaft(app_pb2_grpc.AppServiceServicer):
    def __init__(self, raft_node):
        self.raft_node = raft_node

    def login(self, request, context):
        username = request.username
        password = request.password
        if authenticate_user(username, password):
            token = generate_token(username)
            return app_pb2.LoginResponse(
                status="success",
                token=token
            )
        context.set_code(grpc.StatusCode.UNAUTHENTICATED)
        context.set_details("Invalid credentials")
        return app_pb2.LoginResponse(
            status="failed",
            token=""
        )
    
    def post(self, request, context):
        """Handle write operations through Raft"""
        # Verify token
        username = verify_token(request.token)
        if not username:
            context.set_code(grpc.StatusCode.UNAUTHENTICATED)
            context.set_details('Invalid or expired token')
            return app_pb2.PostResponse()
        
        # Check if this node is the leader
        if not self.raft_node.is_leader():
            # Not the leader - check if we know who the leader is
            leader_addr = self.raft_node.get_leader_address()
            if leader_addr:
                return app_pb2.PostResponse(
                    status="redirect",
                    result=f"LEADER:{leader_addr}"
                )
            else:
                return app_pb2.PostResponse(
                    status="no_leader",
                    result="No leader elected yet, please retry"
                )
        
        # We are the leader - process the request
        op_type = request.type
        data = request.data
        request_id = request.requestId or f"{username}_{time.time()}"
        
        # Check for duplicate request
        cached_result = self.raft_node.check_duplicate_request(request_id)
        if cached_result:
            return app_pb2.PostResponse(
                status=cached_result.get('status', 'ok'),
                result=cached_result.get('result', '')
            )
        
        try:
            # Parse data
            parts = data.split(':', 1)
            if len(parts) != 2:
                return app_pb2.PostResponse(
                    status="error",
                    result="Invalid data format. Use 'product:quantity'"
                )
            
            product, qty_str = parts
            product = product.strip()
            
            if op_type == "add_inventory":
                qty_change = int(qty_str)
                
                # Append to Raft log
                success = self.raft_node.append_log_entry(
                    operation='add_inventory',
                    product=product,
                    qty_change=qty_change,
                    new_qty=0,
                    username=username,
                    request_id=request_id
                )
                
                if not success:
                    return app_pb2.PostResponse(
                        status="error",
                        result="Failed to append to log (lost leadership?)"
                    )
                
                # Wait for commit (with timeout)
                max_wait = 5.0
                start = time.time()
                while time.time() - start < max_wait:
                    with self.raft_node.state_lock:
                        if self.raft_node.commit_index >= len(self.raft_node.log):
                            # Entry is committed
                            current_qty = self.raft_node.inventory.get(product, 0)
                            result_data = {
                                'status': 'added',
                                'result': f"Added {qty_change} units of {product}. Current: {current_qty}"
                            }
                            self.raft_node.cache_request_result(request_id, result_data)
                            return app_pb2.PostResponse(**result_data)
                    time.sleep(0.01)
                
                # Timeout
                return app_pb2.PostResponse(
                    status="timeout",
                    result="Operation timed out (may still succeed)"
                )
            
            elif op_type == "update_inventory":
                new_qty = int(qty_str)
                
                success = self.raft_node.append_log_entry(
                    operation='update_inventory',
                    product=product,
                    qty_change=0,
                    new_qty=new_qty,
                    username=username,
                    request_id=request_id
                )
                
                if not success:
                    return app_pb2.PostResponse(
                        status="error",
                        result="Failed to append to log"
                    )
                
                # Wait for commit
                max_wait = 5.0
                start = time.time()
                while time.time() - start < max_wait:
                    with self.raft_node.state_lock:
                        if self.raft_node.commit_index >= len(self.raft_node.log):
                            result_data = {
                                'status': 'updated',
                                'result': f"Updated {product} to {new_qty} units"
                            }
                            self.raft_node.cache_request_result(request_id, result_data)
                            return app_pb2.PostResponse(**result_data)
                    time.sleep(0.01)
                
                return app_pb2.PostResponse(
                    status="timeout",
                    result="Operation timed out"
                )
            
            else:
                return app_pb2.PostResponse(
                    status="error",
                    result=f"Unknown operation: {op_type}"
                )
        
        except ValueError:
            return app_pb2.PostResponse(
                status="error",
                result="Invalid quantity (must be a number)"
            )
        except Exception as e:
            return app_pb2.PostResponse(
                status="error",
                result=f"Error: {str(e)}"
            )
    
    def get(self, request, context):
        """Handle read operations (can be served by any node)"""
        username = verify_token(request.token)
        if not username:
            context.set_code(grpc.StatusCode.UNAUTHENTICATED)
            context.set_details('Invalid or expired token')
            return app_pb2.GetResponse()
        
        get_type = request.type
        
        if get_type == "get_inventory":
            with self.raft_node.state_lock:
                items = [f"{product}:{qty}" for product, qty in self.raft_node.inventory.items()]
            
            return app_pb2.GetResponse(
                status="ok",
                items=items
            )
        
        else:
            return app_pb2.GetResponse(
                status="error",
                items=[f"Unknown get type: {get_type}"]
            )

def serve(node_id: str, port: int, peers: list, llm_address: str = "127.0.0.1:50054"):
    # Create Raft node (but don't start timers yet!)
    raft_node = RaftNode(node_id, peers, port, llm_server_address=llm_address)
    
    # Create gRPC server
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    
    # Add services
    app_pb2_grpc.add_AppServiceServicer_to_server(AppServiceWithRaft(raft_node), server)
    raft_pb2_grpc.add_RaftServiceServicer_to_server(RaftServicer(raft_node), server)
    
    # Bind to IPv4
    server.add_insecure_port(f'0.0.0.0:{port}')
    
    # ✅ START SERVER FIRST
    server.start()
    print(f"[{node_id}] Server running on port {port}...")
    
    # ✅ THEN start Raft timers (after server is listening)
    print(f"[{node_id}] Starting Raft consensus engine...")
    raft_node.start()
    
    # Wait for termination
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        print(f"[{node_id}] Shutting down...")
        raft_node.running = False
        server.stop(0)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python -m server_app_leader.app_server_raft <node_id>")
        sys.exit(1)
    
    node_id = sys.argv[1]
    
    config = {
        "leader":   {"port": 50051, "peers": ["127.0.0.1:50052", "127.0.0.1:50053"]},
        "follower1":{"port": 50052, "peers": ["127.0.0.1:50051", "127.0.0.1:50053"]},
        "follower2":{"port": 50053, "peers": ["127.0.0.1:50051", "127.0.0.1:50052"]}
    }
    
    if node_id not in config:
        print(f"Unknown node: {node_id}")
        sys.exit(1)
    
    serve(node_id, config[node_id]["port"], config[node_id]["peers"])