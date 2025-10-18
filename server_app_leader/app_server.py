import grpc
from concurrent import futures
import uuid

import app_pb2
import app_pb2_grpc
import llm_pb2
import llm_pb2_grpc


class AppServiceServicer(app_pb2_grpc.AppServiceServicer):
    def __init__(self):
        self.sessions = {}
        self.inventory = {"P1": 10, "P2": 5}
        self.llm_channel = grpc.insecure_channel("localhost:50052")
        self.llm_stub = llm_pb2_grpc.LLMServiceStub(self.llm_channel)

    def login(self, request, context):
        print(f"[App] Login request from {request.username}")
        token = "token123"
        self.sessions[token] = request.username
        return app_pb2.LoginResponse(status="success", token=token)

    def post(self, request, context):
        if request.token not in self.sessions:
            return app_pb2.PostResponse(status="unauthorized", result="")

        print(f"[App] Post received: type={request.type} data={request.data}")
        if request.type == "add_inventory":
            try:
                product, qty_s = request.data.split(":", 1)
                qty = int(qty_s)
                self.inventory[product] = self.inventory.get(product, 0) + qty
                return app_pb2.PostResponse(
                    status="added", result=f"{product}:{self.inventory[product]}"
                )
            except Exception as e:
                return app_pb2.PostResponse(status="bad_request", result=str(e))

        elif request.type == "query_ai":
            llm_req = llm_pb2.LLMRequest(
                requestId=str(uuid.uuid4()),
                query=request.data,
                context=";".join(f"{k}:{v}" for k, v in self.inventory.items()),
            )
            llm_resp = self.llm_stub.getLLMAnswer(llm_req)
            return app_pb2.PostResponse(status="ok", result=llm_resp.answer)

        else:
            return app_pb2.PostResponse(status="unknown_type", result="")

    def get(self, request, context):
        if request.token not in self.sessions:
            return app_pb2.GetResponse(status="unauthorized", items=[])
        if request.type == "inventory":
            items = [f"{k}:{v}" for k, v in self.inventory.items()]
            return app_pb2.GetResponse(status="ok", items=items)
        return app_pb2.GetResponse(status="unknown_type", items=[])


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=5))
    app_pb2_grpc.add_AppServiceServicer_to_server(AppServiceServicer(), server)
    server.add_insecure_port("[::]:50051")
    print("App Server running on port 50051...")
    server.start()
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
