import grpc
import app_pb2
import app_pb2_grpc
import time
import uuid

class RaftClient:
    def __init__(self, initial_servers):
        """
        initial_servers: list like ['127.0.0.1:50051', '127.0.0.1:50052', '127.0.0.1:50053']
        """
        self.servers = initial_servers
        self.leader_address = None
        self.token = None
        self.username = None
    
    def _get_stub(self):
        """Get a stub, trying leader first, then other servers"""
        addresses_to_try = []
        
        # Try known leader first
        if self.leader_address:
            addresses_to_try.append(self.leader_address)
        
        # Then try all servers
        addresses_to_try.extend([s for s in self.servers if s != self.leader_address])
        
        for addr in addresses_to_try:
            try:
                channel = grpc.insecure_channel(addr)
                # Quick connectivity check
                grpc.channel_ready_future(channel).result(timeout=0.5)
                return app_pb2_grpc.AppServiceStub(channel), addr, channel
            except:
                continue
        
        raise Exception("No servers available")
    
    def login(self, username, password):
        """Login and get token"""
        try:
            stub, addr, channel = self._get_stub()
            response = stub.login(app_pb2.LoginRequest(
                username=username,
                password=password
            ))
            channel.close()
            
            if response.status == "success":
                self.token = response.token
                self.username = username
                print(f"Logged in as {username}")
                return True
            else:
                print(f"Login failed: {response.status}")
                return False
        except Exception as e:
            print(f"Login error: {e}")
            return False
    
    def add_inventory(self, product, qty):
        """Add inventory with automatic leader detection and retry"""
        request_id = str(uuid.uuid4())
        max_retries = 3
        
        for attempt in range(max_retries):
            try:
                stub, addr, channel = self._get_stub()
                
                print(f"Attempt {attempt + 1}: Sending to {addr}...")
                
                response = stub.post(app_pb2.PostRequest(
                    token=self.token,
                    type="add_inventory",
                    data=f"{product}:{qty}",
                    requestId=request_id   # removed params
                ), timeout=10.0)
                
                channel.close()
                
                if response.status == "added":
                    self.leader_address = addr  # Cache leader
                    print(f"Added {qty} units of {product}")
                    print(f"   Result: {response.result}")
                    return True
                
                elif response.status == "redirect":
                    # Server told us who the leader is
                    leader_info = response.result
                    if leader_info.startswith("LEADER:"):
                        new_leader = leader_info.replace("LEADER:", "")
                        print(f"Redirecting to leader: {new_leader}")
                        self.leader_address = new_leader
                        continue
                
                elif response.status == "no_leader":
                    print(f"No leader available, election in progress...")
                    self.leader_address = None
                    time.sleep(1.0)
                    continue
                
                elif response.status == "timeout":
                    print(f"Operation timeout (may still succeed)")
                    print(f"   Retrying with same request ID for idempotency...")
                    time.sleep(0.5)
                    continue
                
                else:
                    print(f"Operation failed: {response.status} - {response.result}")
                    return False
                    
            except grpc.RpcError as e:
                if attempt < max_retries - 1:
                    print(f"RPC error, retrying... ({e.code()})")
                    self.leader_address = None
                    time.sleep(0.5)
                else:
                    print(f"All retries failed: {e.details()}")
            
            except Exception as e:
                print(f"Unexpected error: {e}")
                return False
        
        return False
    
    def update_inventory(self, product, new_qty):
        """Update inventory to specific quantity"""
        request_id = str(uuid.uuid4())
        max_retries = 3
        
        for attempt in range(max_retries):
            try:
                stub, addr, channel = self._get_stub()
                
                print(f"Attempt {attempt + 1}: Sending to {addr}...")
                
                response = stub.post(app_pb2.PostRequest(
                    token=self.token,
                    type="update_inventory",
                    data=f"{product}:{new_qty}",
                    requestId=request_id
                ), timeout=10.0)
                
                channel.close()
                
                if response.status == "updated":
                    self.leader_address = addr
                    print(f"Updated {product}")
                    print(f"   Result: {response.result}")
                    return True
                
                elif response.status == "redirect":
                    leader_info = response.result
                    if leader_info.startswith("LEADER:"):
                        new_leader = leader_info.replace("LEADER:", "")
                        print(f"Redirecting to leader: {new_leader}")
                        self.leader_address = new_leader
                        continue
                
                elif response.status == "no_leader":
                    print(f"No leader available, election in progress...")
                    self.leader_address = None
                    time.sleep(1.0)
                    continue
                
                elif response.status == "timeout":
                    print(f"Operation timeout (may still succeed)")
                    time.sleep(0.5)
                    continue
                
                else:
                    print(f"Operation failed: {response.status} - {response.result}")
                    return False
                    
            except grpc.RpcError as e:
                if attempt < max_retries - 1:
                    print(f"RPC error, retrying...")
                    self.leader_address = None
                    time.sleep(0.5)
                else:
                    print(f"All retries failed: {e.details()}")
        
        return False
    
    def get_inventory(self):
        """Get current inventory (can be served by any node)"""
        try:
            stub, addr, channel = self._get_stub()
            response = stub.get(app_pb2.GetRequest(
                token=self.token,
                type="get_inventory"
            ))  # removed params
            channel.close()
            
            if response.status == "ok":
                print(f"\n Current Inventory (from {addr}):")
                if response.items:
                    for item in response.items:
                        product, qty = item.split(":", 1)
                        print(f"   {product}: {qty} units")
                else:
                    print("   (empty)")
                return True
            else:
                print(f"Failed to get inventory: {response.status}")
                return False
                
        except Exception as e:
            print(f"Error: {e}")
            return False

def main():
    print("="*60)
    print("Distributed Inventory System with Raft Consensus")
    print("="*60)
    
    # Connect to all known servers
    servers = ['127.0.0.1:50051', '127.0.0.1:50052', '127.0.0.1:50053']
    client = RaftClient(servers)
    
    # Login
    print("\nLogin required")
    username = input("Username [admin]: ").strip() or "admin"
    password = input("Password [admin123]: ").strip() or "admin123"
    
    if not client.login(username, password):
        print("Login failed. Exiting.")
        return
    
    # Interactive menu
    while True:
        print("\n" + "="*60)
        print("Menu")
        print("="*60)
        print("1. Add Inventory")
        print("2. Update Inventory")
        print("3. View Inventory")
        print("4. Exit")
        print("="*60)
        
        choice = input("\nChoice: ").strip()
        
        if choice == '1':
            product = input("Product name: ").strip()
            if not product:
                print("Product name cannot be empty")
                continue
            try:
                qty = int(input("Quantity to add: ").strip())
                client.add_inventory(product, qty)
            except ValueError:
                print("Invalid quantity")
        
        elif choice == '2':
            product = input("Product name: ").strip()
            if not product:
                print("Product name cannot be empty")
                continue
            try:
                new_qty = int(input("New quantity: ").strip())
                client.update_inventory(product, new_qty)
            except ValueError:
                print("Invalid quantity")
        
        elif choice == '3':
            client.get_inventory()
        
        elif choice == '4':
            print("\nGoodbye!")
            break
        
        else:
            print("Invalid choice")

if __name__ == '__main__':
    main()