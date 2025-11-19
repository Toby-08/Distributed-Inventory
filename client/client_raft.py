import grpc
import raft_pb2
import raft_pb2_grpc
import time
import uuid
import os
from datetime import datetime

class RaftClient:
    def __init__(self, initial_servers, username, password):
        """
        initial_servers: list of "host:port" strings
        """
        self.servers = initial_servers
        self.current_leader = None
        self.request_timeout = 3.0
        self.max_retries = 3
        self.username = username
        self.password = password
    
    def _find_leader(self):
        """Query all servers to find current leader"""
        print("Finding current leader...")
        
        for server_addr in self.servers:
            try:
                channel = grpc.insecure_channel(server_addr)
                stub = raft_pb2_grpc.RaftServiceStub(channel)
                
                response = stub.GetLeaderInfo(
                    raft_pb2.GetLeaderRequest(),
                    timeout=1.0
                )
                
                if response.is_leader:
                    self.current_leader = server_addr
                    print(f"Found leader: {server_addr}")
                    channel.close()
                    return server_addr
                elif response.leader_address:
                    self.current_leader = response.leader_address
                    print(f"Leader is: {response.leader_address} (via {server_addr})")
                    channel.close()
                    return response.leader_address
                
                channel.close()
                
            except grpc.RpcError as e:
                print(f"Error from {server_addr}: {e.code()}")
                continue
            except Exception as e:
                continue
        
        # Fallback: try first server
        print("Could not find leader, defaulting to first server")
        self.current_leader = self.servers[0]
        return self.current_leader
    
    def _get_stub(self):
        """Get gRPC stub for current leader"""
        if not self.current_leader:
            self._find_leader()
        
        channel = grpc.insecure_channel(self.current_leader)
        return raft_pb2_grpc.RaftServiceStub(channel), channel
    
    def add_inventory(self, product, quantity):
        """Add inventory with leader discovery and retries"""
        request_id = str(uuid.uuid4())
        
        for attempt in range(1, self.max_retries + 1):
            try:
                if attempt > 1:
                    print(f"Retrying (attempt {attempt}/{self.max_retries})...")
                    self._find_leader()
                
                stub, channel = self._get_stub()
                
                response = stub.AddInventory(
                    raft_pb2.AddInventoryRequest(
                        username=self.username,
                        product=product,
                        quantity=quantity,
                        request_id=request_id,
                        timestamp=datetime.now().isoformat()
                    ),
                    timeout=self.request_timeout
                )
                
                channel.close()
                
                if response.success:
                    print(f"Success: {response.message}")
                    return True
                else:
                    if "not leader" in response.message.lower():
                        if response.leader_hint:
                            print(f"Redirecting to leader: {response.leader_hint}")
                            self.current_leader = response.leader_hint
                        else:
                            self._find_leader()
                        continue
                    print(f"Failed: {response.message}")
                    return False
                    
            except grpc.RpcError as e:
                if e.code() == grpc.StatusCode.DEADLINE_EXCEEDED:
                    print(f"Timeout on attempt {attempt}")
                elif e.code() == grpc.StatusCode.UNAVAILABLE:
                    print(f"Server unavailable: {self.current_leader}")
                    self.current_leader = None
                else:
                    print(f"RPC Error: {e.code()}")
                
                if attempt < self.max_retries:
                    time.sleep(1)
                    
            except Exception as e:
                print(f"Error: {type(e).__name__}: {e}")
                if attempt < self.max_retries:
                    time.sleep(1)
        
        print("All retry attempts failed")
        return False
    
    def update_inventory(self, product, quantity):
        """Update inventory to absolute quantity"""
        request_id = str(uuid.uuid4())
        
        for attempt in range(1, self.max_retries + 1):
            try:
                if attempt > 1:
                    print(f"Retrying (attempt {attempt}/{self.max_retries})...")
                    self._find_leader()
                
                stub, channel = self._get_stub()
                
                response = stub.UpdateInventory(
                    raft_pb2.UpdateInventoryRequest(
                        username=self.username,
                        product=product,
                        quantity=quantity,
                        request_id=request_id,
                        timestamp=datetime.now().isoformat()
                    ),
                    timeout=self.request_timeout
                )
                
                channel.close()
                
                if response.success:
                    print(f"Success: {response.message}")
                    return True
                else:
                    if "not leader" in response.message.lower():
                        if response.leader_hint:
                            self.current_leader = response.leader_hint
                        else:
                            self._find_leader()
                        continue
                    print(f"Failed: {response.message}")
                    return False
                    
            except grpc.RpcError as e:
                if e.code() == grpc.StatusCode.DEADLINE_EXCEEDED:
                    print(f"Timeout on attempt {attempt}")
                elif e.code() == grpc.StatusCode.UNAVAILABLE:
                    print(f"Server unavailable: {self.current_leader}")
                    self.current_leader = None
                else:
                    print(f"RPC Error: {e.code()}")
                
                if attempt < self.max_retries:
                    time.sleep(1)
                    
            except Exception as e:
                print(f"Error: {type(e).__name__}: {e}")
                if attempt < self.max_retries:
                    time.sleep(1)
        
        print("All retry attempts failed")
        return False
    
    def view_inventory(self):
        """View current inventory with retry logic"""
        for attempt in range(self.max_retries):
            try:
                stub, channel = self._get_stub()
                
                response = stub.GetInventory(
                    raft_pb2.GetInventoryRequest(),
                    timeout=self.request_timeout
                )
                
                channel.close()
                
                print("\n" + "="*40)
                print("Current Inventory")
                print("="*40)
                if response.items:
                    for item in response.items:
                        print(f"  {item.product:20s} : {item.quantity:>5d} units")
                else:
                    print("  (empty)")
                print("="*40 + "\n")
                return True
                
            except grpc.RpcError as e:
                if e.code() == grpc.StatusCode.FAILED_PRECONDITION:
                    if self._find_leader():
                        continue
                    else:
                        print("Could not find leader")
                        return False
                elif e.code() == grpc.StatusCode.UNAVAILABLE:
                    print(f"Leader unavailable on attempt {attempt + 1}")
                    if attempt < self.max_retries - 1:
                        print(f"Retrying (attempt {attempt + 2}/{self.max_retries})...")
                        self._find_leader()
                        time.sleep(0.5)
                else:
                    print(f"Error: {e.details()}")
                    return False
            except Exception as e:
                print(f"Error: {type(e).__name__}: {e}")
                if attempt < self.max_retries - 1:
                    time.sleep(1)
        
        print("Failed to get inventory after multiple attempts")
        return False
    
    def query_llm(self, query):
        """Send a query to the LLM through the current Raft leader only."""
        request_id = str(uuid.uuid4())  # ✅ Unique ID per query

        for attempt in range(1, self.max_retries + 1):
            try:
                # Always ensure we're talking to the leader
                if not self.current_leader or attempt > 1:
                    print(f"Finding leader (attempt {attempt})...")
                    self._find_leader()
                
                if not self.current_leader:
                    print("No leader found — cannot query LLM.")
                    continue
                
                channel = grpc.insecure_channel(self.current_leader)
                stub = raft_pb2_grpc.RaftServiceStub(channel)
                
                response = stub.QueryLLM(
                    raft_pb2.QueryLLMRequest(
                        query=query,
                        username=self.username,
                        request_id=request_id
                    ),
                    timeout=60.0
                )
                
                channel.close()
                
                if response.success:
                    print("\n" + "="*50)
                    print("🤖 AI Response (from leader):")
                    print("="*50)
                    print(response.response)
                    print("="*50 + "\n")
                    return True
                else:
                    # If leader redirects
                    if "not leader" in response.error.lower() or "redirect" in response.error.lower():
                        if hasattr(response, "leader_hint") and response.leader_hint:
                            print(f"Redirecting to leader: {response.leader_hint}")
                            self.current_leader = response.leader_hint
                            continue
                        print("Leader not found in response — rediscovering.")
                        self._find_leader()
                        continue
                    
                    print(f"Query failed: {response.error}")
                    return False
                
            except grpc.RpcError as e:
                print(f"[gRPC] Error while querying LLM: {e.code()}")
                self.current_leader = None  # Force re-discovery
                if attempt < self.max_retries:
                    time.sleep(1)
                    continue
            
            except Exception as e:
                print(f"Error: {type(e).__name__}: {e}")
                if attempt < self.max_retries:
                    time.sleep(1)
        
        print("❌ Could not reach leader for query after multiple attempts.")
        return False


def main():
    # Get credentials
    username = input("Username: ").strip() or "admin"
    password = input("Password: ").strip() or "admin123"
    
    print(f"\nStarting client as: {username}\n")
    
    # Get servers from environment or use defaults
    servers_env = os.getenv("RAFT_SERVERS", "")
    if servers_env:
        initial_servers = [s.strip() for s in servers_env.split(",")]
    else:
        initial_servers = [
            "127.0.0.1:50051",
            "127.0.0.1:50052",
            "127.0.0.1:50053"
        ]
    
    client = RaftClient(initial_servers, username, password)
    
    # Find leader on startup
    client._find_leader()
    
    while True:
        print("\n" + "="*40)
        print("Inventory Management")
        print("="*40)
        print("1. Add Inventory      - Increase quantity")
        print("2. Update Inventory   - Set absolute quantity")
        print("3. View Inventory     - Display all items")
        print("4. Query AI           - Ask questions about inventory")
        print("5. Exit")
        print("="*40)
        
        choice = input("Choice: ").strip()
        
        if choice == "1":
            product = input("Product name: ").strip()
            if not product:
                print("Product name required")
                continue
            try:
                quantity = int(input("Quantity to add: ").strip())
                client.add_inventory(product, quantity)
            except ValueError:
                print("Invalid quantity")
        
        elif choice == "2":
            product = input("Product name: ").strip()
            if not product:
                print("Product name required")
                continue
            try:
                quantity = int(input("New total quantity: ").strip())
                client.update_inventory(product, quantity)
            except ValueError:
                print("Invalid quantity")
        
        elif choice == "3":
            client.view_inventory()
        
        elif choice == "4":
            query = input("\nAsk a question about inventory: ").strip()
            if query:
                client.query_llm(query)
            else:
                print("Query cannot be empty")
        
        elif choice == "5":
            print("\nGoodbye!\n")
            break
        
        else:
            print("Invalid choice (1-5)")


if __name__ == '__main__':
    main()