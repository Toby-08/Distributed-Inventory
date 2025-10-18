import grpc
from concurrent import futures
import llm_pb2
import llm_pb2_grpc

class LLMServiceServicer(llm_pb2_grpc.LLMServiceServicer):
    def getLLMAnswer(self, request, context):
        print(f"[LLM] Received query: {request.query}")
        # Mock AI answer
        answer = f"AI Suggestion based on query '{request.query}' and context '{request.context}'"
        return llm_pb2.LLMResponse(requestId=request.requestId, answer=answer)

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=5))
    llm_pb2_grpc.add_LLMServiceServicer_to_server(LLMServiceServicer(), server)
    server.add_insecure_port('[::]:50052')
    print("LLM Server running on port 50052...")
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    serve()
