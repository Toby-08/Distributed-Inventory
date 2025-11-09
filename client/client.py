import grpc
import app_pb2
import app_pb2_grpc
import time

def connect_with_retry(address, max_retries=5):
    """Try connecting with exponential backoff"""
    for attempt in range(max_retries):
        try:
            channel = grpc.insecure_channel(address)
            stub = app_pb2_grpc.AppServiceStub(channel)
            # Test connection with a dummy call
            return channel, stub
        except grpc.RpcError:
            if attempt < max_retries - 1:
                wait = 2 ** attempt
                print(f"Server not ready, retrying in {wait}s...")
                time.sleep(wait)
            else:
                raise Exception("Could not connect to server")

def run():
    print("Connecting to server...")
    channel, stub = connect_with_retry('localhost:50051')
    
    # Login
    username = input("Username: ")
    password = input("Password: ")
    response = stub.login(app_pb2.LoginRequest(username=username, password=password))
    
    if response.status != "success":
        print("❌ Login failed!")
        return
    
    token = response.token
    print(f"✓ Logged in as {username}\n")

    while True:
        print("\n" + "="*50)
        print("INVENTORY MANAGEMENT SYSTEM")
        print("="*50)
        print("1. Add Inventory      - Increase quantity of existing/new product")
        print("2. Update Inventory   - Set absolute quantity (modify existing)")
        print("3. View Inventory     - Display all products")
        print("4. AI Query           - Ask AI about inventory")
        print("5. Exit")
        print("="*50)
        
        cmd = input("Enter choice (1-5): ").strip()

        try:
            if cmd == "1":
                print("\n--- Add Inventory ---")
                product = input("Product name: ").strip()
                qty = input("Quantity to add: ").strip()
                data = f"{product}:{qty}"
                resp = stub.post(app_pb2.PostRequest(token=token, type="add_inventory", data=data))
                print(f"✓ {resp.status.upper()}: {resp.result}\n")

            elif cmd == "2":
                print("\n--- Update Inventory ---")
                # First show current inventory
                resp = stub.get(app_pb2.GetRequest(token=token, type="inventory", params=""))
                if resp.items:
                    print("Current Inventory:")
                    for item in resp.items:
                        print(f"  • {item}")
                    print()
                
                product = input("Product name to update: ").strip()
                new_qty = input("New quantity: ").strip()
                data = f"{product}:{new_qty}"
                resp = stub.post(app_pb2.PostRequest(token=token, type="update_inventory", data=data))
                
                if resp.status == "not_found":
                    print(f"❌ {resp.result}")
                    print("   Hint: Use 'Add Inventory' to create new products\n")
                else:
                    print(f"✓ {resp.status.upper()}: {resp.result}\n")

            elif cmd == "3":
                print("\n--- Current Inventory ---")
                resp = stub.get(app_pb2.GetRequest(token=token, type="inventory", params=""))
                if not resp.items:
                    print("  (empty)")
                else:
                    for item in resp.items:
                        product, qty = item.split(":")
                        print(f"  • {product:20s} : {qty:>5s} units")
                print()

            elif cmd == "4":
                print("\n--- AI Query ---")
                query = input("Ask AI: ").strip()
                resp = stub.post(app_pb2.PostRequest(token=token, type="query_ai", data=query))
                print(f"\n🤖 AI Response:\n{resp.result}\n")

            elif cmd == "5":
                print("\n👋 Goodbye!")
                break

            else:
                print("❌ Invalid choice. Please enter 1-5.\n")
        
        except grpc.RpcError as e:
            print(f"\n❌ Error: {e.details()}\n")
            if e.code() == grpc.StatusCode.UNAUTHENTICATED:
                print("⚠️  Session expired. Please restart and login again.")
                break
            elif e.code() == grpc.StatusCode.ABORTED:
                print("⚠️  Resource is busy, please try again in a moment.")

if __name__ == "__main__":
    run()
