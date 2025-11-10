import grpc
from concurrent import futures
import llm_pb2
import llm_pb2_grpc
import json
import os
from datetime import datetime

class LLMServicer(llm_pb2_grpc.LLMServiceServicer):
    def __init__(self):
        self.log_file = "llm_server/llm_context_log.jsonl"
        self.logs = []
        self.inventory = {}
        
        # Check for API key (optional)
        self.api_key = os.getenv("OPENAI_API_KEY") or os.getenv("ANTHROPIC_API_KEY")
        self.use_real_llm = bool(self.api_key)
        
        if self.use_real_llm:
            print(f"[LLM] Running with real LLM API")
            # TODO: Initialize OpenAI/Anthropic client here
            # self.llm_client = ...
        else:
            print(f"[LLM] Running in pattern-matching mode (no API key found)")
        
        # Load existing logs
        self._load_logs()
        print(f"[LLM] Loaded {len(self.logs)} log entries")
        if self.inventory:
            print(f"[LLM] Current inventory: {self.inventory}")
    
    def _load_logs(self):
        """Load logs from file"""
        if not os.path.exists(self.log_file):
            return
        
        try:
            with open(self.log_file, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        entry = json.loads(line)
                        self.logs.append(entry)
                        self._apply_to_inventory(entry)
                    except json.JSONDecodeError:
                        continue
        except Exception as e:
            print(f"[LLM] Error loading logs: {e}")
    
    def _apply_to_inventory(self, entry):
        """Apply log entry to inventory"""
        product = entry.get('product')
        if not product:
            return
        
        if entry['operation'] == 'add_inventory':
            self.inventory[product] = self.inventory.get(product, 0) + entry.get('qty_change', 0)
        elif entry['operation'] == 'update_inventory':
            self.inventory[product] = entry.get('new_qty', 0)
    
    def _persist_log(self, entry):
        """Persist log entry to file"""
        try:
            os.makedirs(os.path.dirname(self.log_file), exist_ok=True)
            with open(self.log_file, 'a', encoding='utf-8') as f:
                f.write(json.dumps(entry) + '\n')
                f.flush()
        except Exception as e:
            print(f"[LLM] Error persisting log: {e}")
    
    def SyncLogs(self, request, context):
        """Sync all logs from leader"""
        print(f"[LLM] Received sync request from {request.leader_id} with {len(request.logs)} logs")
        
        try:
            # Clear existing data
            self.logs = []
            self.inventory = {}
            
            # Rewrite log file
            if os.path.exists(self.log_file):
                os.remove(self.log_file)
            
            # Apply all logs
            for log_entry in request.logs:
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
                
                self.logs.append(entry)
                self._apply_to_inventory(entry)
                self._persist_log(entry)
            
            print(f"[LLM] Synced {len(self.logs)} logs. Inventory: {self.inventory}")
            
            return llm_pb2.SyncLogsResponse(
                success=True,
                logs_synced=len(request.logs),
                message=f"Synced {len(request.logs)} logs"
            )
        
        except Exception as e:
            print(f"[LLM] Sync error: {e}")
            return llm_pb2.SyncLogsResponse(
                success=False,
                logs_synced=0,
                message=f"Error: {str(e)}"
            )
    
    def AppendLog(self, request, context):
        """Append a single log entry"""
        try:
            log_entry = request.log
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
            
            self.logs.append(entry)
            self._apply_to_inventory(entry)
            self._persist_log(entry)
            
            print(f"[LLM] Appended log {entry['index']}: {entry['operation']} {entry['product']}")
            
            return llm_pb2.AppendLogResponse(
                success=True,
                message="Log appended"
            )
        
        except Exception as e:
            return llm_pb2.AppendLogResponse(
                success=False,
                message=f"Error: {str(e)}"
            )
    
    def QueryInventory(self, request, context):
        """Answer natural language queries about inventory"""
        try:
            query = request.query.lower().strip()
            username = request.username
            
            print(f"[LLM] Query from {username}: {query}")
            
            # Use real LLM if available, otherwise use pattern matching
            if self.use_real_llm:
                response = self._query_real_llm(query)
            else:
                response = self._process_query_pattern_matching(query)
            
            return llm_pb2.QueryResponse(
                success=True,
                response=response,
                error=""
            )
        
        except Exception as e:
            print(f"[LLM] Query error: {e}")
            return llm_pb2.QueryResponse(
                success=False,
                response="",
                error=str(e)
            )
    
    def _query_real_llm(self, query: str) -> str:
        """Query real LLM API (OpenAI/Anthropic) - TODO: Implement"""
        # This is a placeholder for future real LLM integration
        # For now, fall back to pattern matching
        print("[LLM] Real LLM not yet implemented, using pattern matching")
        return self._process_query_pattern_matching(query)
    
    def _process_query_pattern_matching(self, query: str) -> str:
        """Process natural language query using simple pattern matching"""
        query_lower = query.lower()
        
        # List all inventory
        if any(word in query_lower for word in ["list", "show all", "what do we have", "inventory", "everything"]):
            if not self.inventory:
                return "The inventory is currently empty."
            
            response = "Current Inventory:\n" + "="*40 + "\n"
            for product, qty in sorted(self.inventory.items()):
                response += f"  • {product}: {qty} units\n"
            response += "="*40
            return response
        
        # Check specific product
        for product in self.inventory.keys():
            if product.lower() in query_lower:
                qty = self.inventory[product]
                return f"{product}: {qty} units in stock"
        
        # Total items
        if any(word in query_lower for word in ["total", "how many", "count"]):
            total = sum(self.inventory.values())
            num_products = len(self.inventory)
            return f"Total: {total} units across {num_products} product{'s' if num_products != 1 else ''}"
        
        # Low stock (example: < 10 units)
        if any(word in query_lower for word in ["low stock", "running low", "shortage", "need to order"]):
            low_stock = {p: q for p, q in self.inventory.items() if q < 10}
            if not low_stock:
                return "No products are running low on stock (all products have ≥10 units)."
            
            response = "Low Stock Alert (< 10 units):\n" + "="*40 + "\n"
            for product, qty in sorted(low_stock.items()):
                response += f"  • {product}: {qty} units\n"
            response += "="*40
            return response
        
        # High stock
        if any(word in query_lower for word in ["high stock", "most", "highest"]):
            if not self.inventory:
                return "No inventory data available."
            
            max_product = max(self.inventory.items(), key=lambda x: x[1])
            return f"Highest stock: {max_product[0]} with {max_product[1]} units"
        
        # Recent changes
        if any(word in query_lower for word in ["recent", "latest", "last", "history"]):
            if not self.logs:
                return "No recent activity recorded."
            
            recent = self.logs[-5:]  # Last 5 entries
            response = "Recent Inventory Changes:\n" + "="*40 + "\n"
            for entry in reversed(recent):
                product = entry['product']
                user = entry.get('username', 'unknown')
                timestamp = entry.get('timestamp', '')
                
                if entry['operation'] == 'add_inventory':
                    change = entry.get('qty_change', 0)
                    response += f" {user} added {change} {product}\n"
                else:
                    new_qty = entry.get('new_qty', 0)
                    response += f"   {user} updated {product} to {new_qty}\n"
                
                if timestamp:
                    response += f"      ({timestamp[:19]})\n"
            response += "="*40
            return response
        
        # Empty/zero stock
        if any(word in query_lower for word in ["empty", "zero", "out of stock", "depleted"]):
            empty = {p: q for p, q in self.inventory.items() if q == 0}
            if not empty:
                return "No products are out of stock."
            
            response = "Out of Stock:\n" + "="*40 + "\n"
            for product in sorted(empty.keys()):
                response += f"  • {product}: 0 units\n"
            response += "="*40
            return response
        
        # Help/Instructions
        if any(word in query_lower for word in ["help", "what can you do", "commands"]):
            return (
                " AI Inventory Assistant - Available Queries:\n"
                + "="*50 + "\n"
                + " Inventory Info:\n"
                + "  • 'Show all inventory' or 'List everything'\n"
                + "  • 'How much [product] do we have?'\n"
                + "  • 'What's the total?'\n\n"
                + " Stock Alerts:\n"
                + "  • 'What's running low?'\n"
                + "  • 'Show out of stock items'\n"
                + "  • 'Which product has the most stock?'\n\n"
                + " Activity:\n"
                + "  • 'Show recent changes'\n"
                + "  • 'What's the latest activity?'\n"
                + "="*50
            )
        
        # Default response - summarize current state
        total = sum(self.inventory.values())
        num_products = len(self.inventory)
        
        return (
            f" You asked: '{query}'\n\n"
            f" Current Inventory Summary:\n"
            + "="*40 + "\n"
            f"  • Total products: {num_products}\n"
            f"  • Total units: {total}\n\n"
            f" Try asking:\n"
            f"  • 'Show all inventory'\n"
            f"  • 'How much [product] do we have?'\n"
            f"  • 'What's running low?'\n"
            f"  • 'Show recent changes'\n"
            f"  • 'help' for more options\n"
            + "="*40
        )


def serve():
    """Start LLM gRPC server"""
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    llm_pb2_grpc.add_LLMServiceServicer_to_server(LLMServicer(), server)
    
    port = "50054"
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    
    print(f"[LLM] Server started on port {port}")
    print("[LLM] Ready to receive queries and log syncs")
    
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        print("\n[LLM] Shutting down...")
        server.stop(0)


if __name__ == "__main__":
    serve()
