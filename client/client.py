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
            # Test connection
            stub.login(app_pb2.LoginRequest(username="test", password="test"))
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
        print("Login failed!")
        return
    
    token = response.token
    print(f"✓ Logged in as {username}\n")

    while True:
        print("Options: add_inventory(1) | get_inventory(2) | AI_query(3) | exit(00)")
        cmd = input("→ ").strip().lower()

        try:
            if cmd in ["add_inventory", "1"]:
                product = input("Product: ")
                qty = input("Quantity: ")
                data = f"{product}:{qty}"
                resp = stub.post(app_pb2.PostRequest(token=token, type="add_inventory", data=data))
                print(f"✓ {resp.status}: {resp.result}\n")

            elif cmd in ["get_inventory", "2"]:
                resp = stub.get(app_pb2.GetRequest(token=token, type="inventory", params=""))
                print(f"Inventory ({resp.status}):")
                for item in resp.items:
                    print(f"  • {item}")
                print()

            elif cmd in ["ai_query", "3"]:
                query = input("Query: ")
                resp = stub.post(app_pb2.PostRequest(token=token, type="query_ai", data=query))
                print(f"AI: {resp.result}\n")

            elif cmd in ["exit", "00"]:
                print("Goodbye!")
                break

            else:
                print("❌ Invalid command\n")
        
        except grpc.RpcError as e:
            print(f"❌ Error: {e.details()}\n")
            if e.code() == grpc.StatusCode.UNAUTHENTICATED:
                print("Session expired. Please restart.")
                break

if __name__ == "__main__":
    run()
