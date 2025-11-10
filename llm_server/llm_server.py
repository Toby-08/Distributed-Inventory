import grpc
from concurrent import futures
from datetime import datetime
import json
import os

import llm_pb2
import llm_pb2_grpc

# Try to import anthropic, but allow server to run without it
try:
    import anthropic
    ANTHROPIC_AVAILABLE = True
except ImportError:
    ANTHROPIC_AVAILABLE = False
    print("  Warning: anthropic package not installed. LLM responses will be mocked.")
    print("   Install with: pip install anthropic")

class LLMService(llm_pb2_grpc.LLMServiceServicer):
    def __init__(self):
        # Get API key from environment variable (safer than hardcoding)
        self.api_key = os.environ.get("ANTHROPIC_API_KEY", "")
        
        # Initialize client only if API key is available
        self.client = None
        if ANTHROPIC_AVAILABLE and self.api_key:
            try:
                self.client = anthropic.Anthropic(api_key=self.api_key)
                print("[LLM]  Claude API initialized")
            except Exception as e:
                print(f"[LLM]   Failed to initialize Claude API: {e}")
        else:
            if not ANTHROPIC_AVAILABLE:
                print("[LLM]   Running in MOCK mode (anthropic not installed)")
            elif not self.api_key:
                print("[LLM]   Running in MOCK mode (no API key found)")
                print("[LLM]  Set ANTHROPIC_API_KEY environment variable to enable real LLM")
        
        self.log_file = "llm_server/llm_context_log.jsonl"
        self.logs = []  # In-memory log storage
        
        self._ensure_log_file()
        self._load_logs()
    
    def _ensure_log_file(self):
        """Ensure log file exists"""
        os.makedirs(os.path.dirname(self.log_file), exist_ok=True)
        if not os.path.exists(self.log_file):
            with open(self.log_file, 'w') as f:
                pass
    
    def _load_logs(self):
        """Load existing logs from disk"""
        try:
            if os.path.exists(self.log_file):
                with open(self.log_file, 'r') as f:
                    for line in f:
                        if line.strip():
                            entry = json.loads(line)
                            self.logs.append(entry)
                print(f"[LLM] Loaded {len(self.logs)} historical log entries")
        except Exception as e:
            print(f"[LLM] Error loading logs: {e}")
    
    def _persist_log(self, log_entry: dict):
        """Persist a single log entry to disk"""
        try:
            with open(self.log_file, 'a') as f:
                f.write(json.dumps(log_entry) + '\n')
        except Exception as e:
            print(f"[LLM] Error persisting log: {e}")
    
    def _build_context(self) -> str:
        """Build context string from all logs"""
        if not self.logs:
            return "No inventory operations yet."
        
        context_lines = ["Recent inventory operations:"]
        # Take last 50 logs for context (to avoid token limits)
        recent_logs = self.logs[-50:]
        
        for log in recent_logs:
            op = log['operation']
            product = log['product']
            username = log.get('username', 'unknown')
            timestamp = log.get('timestamp', '')
            
            if op == 'add_inventory':
                qty = log.get('qty_change', 0)
                context_lines.append(f"- {username} added {qty} units of {product} at {timestamp}")
            elif op == 'update_inventory':
                new_qty = log.get('new_qty', 0)
                context_lines.append(f"- {username} updated {product} to {new_qty} units at {timestamp}")
        
        return "\n".join(context_lines)
    
    def _generate_mock_response(self, query: str, context: str) -> str:
        """Generate a mock response when Claude API is not available"""
        return f"""[MOCK LLM RESPONSE]

Query: {query}

Based on the context, here's a simulated response:
- Total operations logged: {len(self.logs)}
- Context provided: {len(context)} characters

To enable real LLM responses:
1. Install anthropic: pip install anthropic
2. Set API key: set ANTHROPIC_API_KEY=your-key-here (Windows) or export ANTHROPIC_API_KEY=your-key-here (Linux/Mac)
3. Restart LLM server

Current context summary:
{context[:500]}...
"""
    
    # New: Bulk sync logs from leader
    def SyncLogs(self, request, context):
        """Handle bulk log sync from Raft leader"""
        print(f"[LLM] Receiving {len(request.logs)} logs from leader {request.leader_id} (term {request.term})")
        
        synced_count = 0
        for log_entry in request.logs:
            # Store in context log
            entry = {
                'term': log_entry.term,
                'index': log_entry.index,
                'operation': log_entry.operation,
                'product': log_entry.product,
                'qty_change': log_entry.qty_change,
                'new_qty': log_entry.new_qty,
                'username': log_entry.username,
                'timestamp': log_entry.timestamp,
                'request_id': log_entry.request_id
            }
            
            # Check if already exists
            if log_entry.index <= len(self.logs):
                # Update existing
                self.logs[log_entry.index - 1] = entry
            else:
                # Append new
                self.logs.append(entry)
            
            # Persist to disk
            with open('llm_server/llm_context_log.jsonl', 'a', encoding='utf-8') as f:
                f.write(json.dumps(entry) + '\n')
            
            synced_count += 1
        
        print(f"[LLM] Successfully synced {synced_count} logs from {request.leader_id}")
        
        return llm_pb2.SyncLogsResponse(
            success=True,
            logs_synced=synced_count,
            message=f"Synced {synced_count} logs"
        )
    
    # New: Append single log entry
    def AppendLog(self, request, context):
        """Append a single log entry from leader"""
        try:
            log_entry = {
                'term': request.log.term,
                'index': request.log.index,
                'operation': request.log.operation,
                'product': request.log.product,
                'qty_change': request.log.qty_change,
                'new_qty': request.log.new_qty,
                'username': request.log.username,
                'timestamp': request.log.timestamp,
                'request_id': request.log.request_id
            }
            
         
            if not any(l['index'] == log_entry['index'] and l['term'] == log_entry['term'] for l in self.logs):
                self.logs.append(log_entry)
                self._persist_log(log_entry)
                print(f"[LLM] Appended log entry: {log_entry['operation']} {log_entry['product']}")
            
            return llm_pb2.AppendLogResponse(success=True)
            
        except Exception as e:
            print(f"[LLM] Error appending log: {e}")
            return llm_pb2.AppendLogResponse(success=False)
    
    def GetResponse(self, request, context):
        """Generate LLM response based on query and current context"""
        try:
            # Build context from logs
            system_context = self._build_context()
            
            # Combine with user's additional context
            full_context = f"{system_context}\n\nUser context: {request.context}" if request.context else system_context
            
            # Use Claude API if available, otherwise mock response
            if self.client:
                message = self.client.messages.create(
                    model="claude-sonnet-4-20250514",
                    max_tokens=1024,
                    messages=[
                        {
                            "role": "user",
                            "content": f"Context:\n{full_context}\n\nQuery: {request.query}"
                        }
                    ]
                )
                
                response_text = message.content[0].text
                print(f"[LLM] Generated Claude response for query: {request.query[:50]}...")
            else:
                # Mock response
                response_text = self._generate_mock_response(request.query, full_context)
                print(f"[LLM] Generated MOCK response for query: {request.query[:50]}...")
            
            return llm_pb2.LLMResponse(
                response=response_text,
                success=True
            )
            
        except Exception as e:
            error_msg = f"Error generating response: {str(e)}"
            print(f"[LLM] Error generating response: {error_msg}")
            return llm_pb2.LLMResponse(
                response=error_msg,
                success=False
            )



def serve():
    """Start the LLM gRPC server"""
    print("[LLM] Starting LLM Server...")
    
    # Check for API key
    api_key = os.environ.get('ANTHROPIC_API_KEY')
    if not api_key:
        print("[LLM] Running in MOCK mode (no API key found)")
        print("[LLM] Set ANTHROPIC_API_KEY environment variable to enable real LLM")
    else:
        print("[LLM] API key found - Real LLM mode enabled")
    
    # Create service
    llm_service = LLMService()
    
    # Create gRPC server
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    
    # CRITICAL: Register the service
    llm_pb2_grpc.add_LLMServiceServicer_to_server(llm_service, server)
    
    # Bind to port
    server.add_insecure_port('0.0.0.0:50054')
    server.start()
    
    print(f"[LLM] Server running on port 50054...")
    print(f"[LLM] Ready to receive log sync from Raft leader")
    
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        print("\n[LLM] Shutting down gracefully...")
        server.stop(0)

if __name__ == '__main__':
    serve()
