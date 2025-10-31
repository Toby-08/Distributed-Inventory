import grpc
from concurrent import futures
import json
import os
from datetime import datetime

import llm_pb2
import llm_pb2_grpc

class LLMServiceServicer(llm_pb2_grpc.LLMServiceServicer):
    def __init__(self):
        self.log_file = "llm_server/llm_context_log.jsonl"
        self._ensure_log_file()
        # Load existing logs into memory for context
        self.context_history = self._load_logs()

    def _ensure_log_file(self):
        os.makedirs(os.path.dirname(self.log_file), exist_ok=True)
        if not os.path.exists(self.log_file):
            with open(self.log_file, 'w') as f:
                pass

    def _load_logs(self):
        """Load all previous logs for context"""
        history = []
        if os.path.exists(self.log_file):
            with open(self.log_file, 'r') as f:
                for line in f:
                    if line.strip():
                        history.append(json.loads(line))
        print(f"[LLM] Loaded {len(history)} historical log entries")
        return history

    def _append_log(self, update: dict):
        """Persist new update to log"""
        with open(self.log_file, 'a') as f:
            f.write(json.dumps(update) + '\n')
        self.context_history.append(update)

    def getLLMAnswer(self, request, context):
        # Check if this is a log update from leader
        if request.query == "__LOG_UPDATE__":
            # Parse: "add_inventory:Product:qty_change:new_qty"
            parts = request.context.split(":", 3)
            if len(parts) == 4:
                log_entry = {
                    "timestamp": datetime.utcnow().isoformat(),
                    "operation": parts[0],
                    "product": parts[1],
                    "qty_change": int(parts[2]),
                    "new_qty": int(parts[3])
                }
                self._append_log(log_entry)
                print(f"[LLM] Received & stored update: {log_entry}")
                return llm_pb2.LLMResponse(
                    requestId=request.requestId,
                    answer="LOG_STORED"
                )
        
        # Normal query processing with full context
        print(f"[LLM] Query: {request.query}")
        print(f"[LLM] Current context: {request.context}")
        print(f"[LLM] Historical logs count: {len(self.context_history)}")
        
        # Build rich context from history + current state
        context_summary = self._build_context_summary(request.context)
        
        # Mock AI answer (replace with actual LLM call)
        answer = f"Based on {len(self.context_history)} historical operations and current inventory ({request.context}), here's my analysis: {context_summary}"
        
        return llm_pb2.LLMResponse(
            requestId=request.requestId,
            answer=answer
        )

    def _build_context_summary(self, current_state: str):
        """Combine historical logs with current state for prediction"""
        if not self.context_history:
            return "No historical data available."
        
        # Example: summarize recent trends
        recent = self.context_history[-10:]  # Last 10 operations
        operations_count = len(recent)
        products_modified = set(log['product'] for log in recent)
        
        summary = f"Recent activity: {operations_count} operations on {len(products_modified)} products. "
        summary += f"Current state: {current_state}. "
        summary += f"Total historical operations: {len(self.context_history)}."
        
        return summary

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=5))
    llm_pb2_grpc.add_LLMServiceServicer_to_server(LLMServiceServicer(), server)
    server.add_insecure_port('[::]:50052')
    print("LLM Server running on port 50052...")
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    serve()
