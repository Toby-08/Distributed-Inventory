import grpc
from concurrent import futures
import threading
import uuid

import app_pb2
import app_pb2_grpc
import llm_pb2
import llm_pb2_grpc
from .auth import generate_token, verify_token


class AppServiceServicer(app_pb2_grpc.AppServiceServicer):
    def __init__(self):
        self.inventory = {"P1": 10, "P2": 5}
        self.inventory_lock = threading.RLock()  # Thread-safe access
        self.llm_channel = grpc.insecure_channel("localhost:50052")
        self.llm_stub = llm_pb2_grpc.LLMServiceStub(self.llm_channel)

        # Mock user database 
        self.users = {"user": "123", "admin": "admin123", "jenil": "123", "kunj": "123", "chakri": "123"}

    def login(self, request, context):
        print(f"[App] Login attempt from {request.username}")

        # Verify credentials
        if request.username not in self.users or self.users[request.username] != request.password:
            return app_pb2.LoginResponse(status="unauthorized", token="")

        # Generate JWT
        token = generate_token(request.username)
        print(f"[App] Login successful for {request.username}")
        return app_pb2.LoginResponse(status="success", token=token)

    def _verify_auth(self, token: str, context):
        """Helper to verify token"""
        try:
            username = verify_token(token)
            return username
        except ValueError as e:
            context.set_code(grpc.StatusCode.UNAUTHENTICATED)
            context.set_details(str(e))
            return None

    def post(self, request, context):
        username = self._verify_auth(request.token, context)
        if not username:
            return app_pb2.PostResponse(status="unauthorized", result="")

        print(f"[App] Post from {username}: type={request.type}")

        if request.type == "add_inventory":
            try:
                product, qty_s = request.data.split(":", 1)
                qty = int(qty_s)

                # Thread-safe inventory update
                with self.inventory_lock:
                    self.inventory[product] = self.inventory.get(product, 0) + qty
                    new_qty = self.inventory[product]

                print(
                    f"[App] {username} added {qty} units of {product}. New total: {new_qty}"
                )
                return app_pb2.PostResponse(status="added", result=f"{product}:{new_qty}")
            except Exception as e:
                return app_pb2.PostResponse(status="bad_request", result=str(e))

        elif request.type == "query_ai":
            # Thread-safe context snapshot
            with self.inventory_lock:
                context_str = ";".join(f"{k}:{v}" for k, v in self.inventory.items())

            llm_req = llm_pb2.LLMRequest(
                requestId=str(uuid.uuid4()),
                query=request.data,
                context=context_str,
            )
            llm_resp = self.llm_stub.getLLMAnswer(llm_req)
            return app_pb2.PostResponse(status="ok", result=llm_resp.answer)

        else:
            return app_pb2.PostResponse(status="unknown_type", result="")

    def get(self, request, context):
        username = self._verify_auth(request.token, context)
        if not username:
            return app_pb2.GetResponse(status="unauthorized", items=[])

        if request.type == "inventory":
            with self.inventory_lock:
                items = [f"{k}:{v}" for k, v in self.inventory.items()]
            return app_pb2.GetResponse(status="ok", items=items)

        return app_pb2.GetResponse(status="unknown_type", items=[])


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))  # Handle multiple clients
    app_pb2_grpc.add_AppServiceServicer_to_server(AppServiceServicer(), server)
    server.add_insecure_port("[::]:50051")
    print("App Server running on port 50051...")
    server.start()
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
