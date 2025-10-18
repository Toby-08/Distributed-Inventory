import grpc
import app_pb2
import app_pb2_grpc

def run():
    channel = grpc.insecure_channel('localhost:50051')
    stub = app_pb2_grpc.AppServiceStub(channel)

    # Login
    response = stub.login(app_pb2.LoginRequest(username="user", password="123"))
    token = response.token
    print(f"Logged in as {response.status}, token={token}")

    while True:
        print("\nOptions: add_inventory(1) | get_inventory(2) | AI_query(3) | exit(00)")
        cmd = input("Enter command: ").strip().lower()  # alias-friendly

        if cmd == "add_inventory" or cmd == "1":
            product = input("Product name: ")
            qty = input("Quantity: ")
            data = f"{product}:{qty}"
            resp = stub.post(app_pb2.PostRequest(token=token, type="add_inventory", data=data))
            print("Server:", resp.status)

        elif cmd == "get_inventory" or cmd == "2":
            resp = stub.get(app_pb2.GetRequest(token=token, type="inventory", params=""))
            print("Inventory:", resp.items)

        elif cmd == "ai_query" or cmd == "3":
            query = input("Enter AI query: ")
            resp = stub.post(app_pb2.PostRequest(token=token, type="query_ai", data=query))
            print("AI Response:", resp.result)  # changed

        elif cmd == "exit" or cmd == "00":
            print("Goodbye!")
            break

        else:
            print("Invalid command")

if __name__ == "__main__":
    run()
