"""
Application Service - Handles client requests (login, signup, inventory operations)
Implements the AppService from app.proto
"""
import grpc
import json
import os, traceback
import uuid
from datetime import datetime, timezone
import app_pb2, app_pb2_grpc, llm_pb2, llm_pb2_grpc
from server_app_leader.auth import generate_token, verify_token, authenticate_user, register_user
from server_app_leader.raft_node import NodeState


class AppServicer(app_pb2_grpc.AppServiceServicer):
    """Handles client application requests"""
    
    def __init__(self, raft_node):
        self.raft_node = raft_node
    
    def _is_leader(self):
        """Check if this node is the current leader"""
        return self.raft_node.state == NodeState.LEADER
    
    def _get_leader_address(self):
        """Return leader address if known"""
        if self.raft_node.leader_id and self.raft_node.leader_id != self.raft_node.node_id:
            # Return the peer we think is leader
            for peer in self.raft_node.peers:
                if self.raft_node.leader_id in peer:
                    return peer
        return ""
    
    def login(self, request, context):
        """Handle login requests"""
        username = request.username
        password = request.password
        
        # Verify credentials
        user = authenticate_user(username, password)
        if not user:
            return app_pb2.LoginResponse(
                status="error",
                message="Invalid credentials",
                token=""
            )
        
        # Generate token (no role needed)
        token = generate_token(username)
        
        return app_pb2.LoginResponse(
            status="success",
            message="Login successful",
            token=token
        )
    
    def signup(self, request, context):
        """Handle user signup"""
        username = request.username
        password = request.password
        
        print(f"[APP] Signup request: {username}")
        
        if not self._is_leader():
            leader_addr = self._get_leader_address()
            print(f"[APP] Not leader, redirecting to {leader_addr} for signup")
            return app_pb2.SignupResponse(
                status="error",
                message="Not leader",
                token=""
            )
        
        # Use auth.py to register (no role)
        success, message = register_user(username, password)
        
        if success:
            print(f"[APP] Signup successful for: {username}")
            # Generate token for new user
            token = generate_token(username)
            return app_pb2.SignupResponse(
                status="success",
                message=message,
                token=token
            )
        else:
            print(f"[APP] Signup failed for: {username} - {message}")
            return app_pb2.SignupResponse(
                status="error",
                message=message,
                token=""
            )
    
    def get(self, request, context):
        """Handle GET requests (read inventory)"""

        token = request.token
        
        # Verify token
        username = verify_token(token)
        if not username:
            return app_pb2.GetResponse(
                status="error",
                message="Invalid or expired token",
                inventory=[]  # Changed from items to inventory
            )
        
        print(f"[APP] Get inventory request from: {username}")
        
        # Only leader serves reads to ensure consistency
        if not self._is_leader():
            leader_addr = self._get_leader_address()
            print(f"[APP] Not leader, redirecting to {leader_addr} for get")
            return app_pb2.GetResponse(
                status="error",
                message="Not leader",
                inventory=[],  # Changed from items to inventory
                leader_address=leader_addr
            )
        
        # Read inventory from Raft state
        with self.raft_node.state_lock:
            inventory = self.raft_node.inventory.copy()
        
        inventory_items = []
        for product, quantity in inventory.items():
            inventory_items.append(app_pb2.InventoryItem(
                product=product,
                quantity=quantity
            ))
        
        print(f"[APP] Returning {len(inventory_items)} inventory items to {username}")
        
        return app_pb2.GetResponse(
            status="success",
            message="",
            inventory=inventory_items  # Changed from items to inventory
        )
    
    def post(self, request, context):
        """Handle POST requests (update inventory)"""
        token = request.token
        operation = request.operation
        product = request.product
        quantity = request.quantity
        
        # Verify token
        username = verify_token(token)
        if not username:
            return app_pb2.PostResponse(
                status="error",
                message="Invalid or expired token"
            )
        
        print(f"[APP] Inventory update from {username}: {operation} {product} x{quantity}")
        
        # Only leader handles writes
        if not self._is_leader():
            leader_addr = self._get_leader_address()
            print(f"[APP] Not leader, redirecting to {leader_addr}")
            return app_pb2.PostResponse(
                status="error",
                message="Not leader",
                leader_address=leader_addr
            )
        
        # Calculate new quantity based on operation
        current_qty = self.raft_node.inventory.get(product, 0)
        
        if operation == "add_inventory":
            new_qty = current_qty + quantity
            qty_change = quantity
        elif operation == "update_inventory":
            new_qty = quantity
            qty_change = quantity - current_qty
        else:
            return app_pb2.PostResponse(
                status="error",
                message=f"Unknown operation: {operation}"
            )
        
        # Append to Raft log

        request_id = str(uuid.uuid4())
        
        success = self.raft_node.append_log_entry(
            operation=operation,
            product=product,
            qty_change=qty_change,
            new_qty=new_qty,
            username=username,
            request_id=request_id
        )
        
        if success:
            # Wait for commit with polling (max 2 seconds)
            import time
            max_wait = 2.0
            poll_interval = 0.05
            waited = 0.0
            
            while waited < max_wait:
                with self.raft_node.state_lock:
                    # Check if entry is committed
                    if len(self.raft_node.log) > 0:
                        last_entry = self.raft_node.log[-1]
                        if last_entry.get('request_id') == request_id and \
                           last_entry['index'] <= self.raft_node.commit_index:
                            print(f"[APP] Entry {last_entry['index']} committed after {waited:.3f}s")
                            break
                
                time.sleep(poll_interval)
                waited += poll_interval
            
            if waited >= max_wait:
                print(f"[APP] Warning: Entry may not be committed yet after {max_wait}s")
            
            return app_pb2.PostResponse(
                status="success",
                message=f"{operation} successful: {product} = {new_qty}"
            )
        else:
            return app_pb2.PostResponse(
                status="error",
                message="Failed to replicate operation"
            )
    
    def query(self, request, context):
        """Handle LLM query requests"""
        token = request.token
        query = request.query
        
        # Verify token
        username = verify_token(token)
        if not username:
            return app_pb2.QueryResponse(
                status="error",
                answer="Invalid or expired token"
            )
        
        print(f"[APP] LLM query from {username}: {query}")
        
        # Forward to LLM server
        try:
            llm_channel = grpc.insecure_channel(self.raft_node.llm_server_address)
            llm_stub = llm_pb2_grpc.LLMServiceStub(llm_channel)
            
            # Use QueryInventory RPC (not Query)
            response = llm_stub.QueryInventory(llm_pb2.QueryRequest(
                query=query,
                username=username,
                request_id=str(uuid.uuid4())
            ), timeout=60.0)  
            
            llm_channel.close()
            
            if response.success:
                return app_pb2.QueryResponse(
                    status="success",
                    answer=response.response
                )
            else:
                return app_pb2.QueryResponse(
                    status="error",
                    answer=f"LLM error: {response.error}"
                )
            
        except Exception as e:
            print(f"[APP] LLM query error: {e}")
            traceback.print_exc()
            return app_pb2.QueryResponse(
                status="error",
                answer=f"LLM service unavailable: {str(e)}"
            )