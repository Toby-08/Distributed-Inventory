# llm_server.py - Enhanced with Gemini AI

import grpc
from concurrent import futures
import llm_pb2
import llm_pb2_grpc
import json
import os
from datetime import datetime, timedelta
from collections import defaultdict
import re
import requests

# Gemini API
try:
    import google.generativeai as genai
    GEMINI_AVAILABLE = True
except ImportError:
    GEMINI_AVAILABLE = False
    print("[LLM] Warning: google-generativeai not installed. Run: pip install google-generativeai")


class LLMServicer(llm_pb2_grpc.LLMServiceServicer):
    def __init__(self):
        self.log_file = "llm_server/llm_context_log.jsonl"
        self.logs = []
        self.inventory = {}
        # Track recent request_ids to prevent duplicate Gemini calls
        self._recent_requests = set()
        self._cached_responses = {}

        
        # Initialize Gemini
        self.gemini_model = None
        self.use_gemini = False
        self._initialize_gemini()
        
        # Load existing logs
        self._load_logs()
        print(f"[LLM] Loaded {len(self.logs)} log entries")
        if self.inventory:
            print(f"[LLM] Current inventory: {self.inventory}")
    
    def _initialize_gemini(self):
        """Initialize Gemini API if key available"""
        api_key = os.getenv("GEMINI_API_KEY")
        
        if not api_key and GEMINI_AVAILABLE:
            print(f"[LLM] No GEMINI_API_KEY found. Set with: export GEMINI_API_KEY='your-key'")
            print(f"[LLM] Get free key at: https://makersuite.google.com/app/apikey")
            print(f"[LLM] Running in pattern-matching mode")
            return
        
        if api_key and GEMINI_AVAILABLE:
            try:
                genai.configure(api_key=api_key, client_options={"api_endpoint": "https://generativelanguage.googleapis.com/v1/"})
                self.gemini_model = genai.GenerativeModel('models/gemini-2.5-flash-lite')
                self.use_gemini = True
                print(f"[LLM] ✅ Gemini API initialized (model: gemini-2.5-flash)")
            except Exception as e:
                print(f"[LLM] ❌ Gemini initialization failed: {e}")
                print(f"[LLM] Falling back to pattern-matching mode")
        else:
            print(f"[LLM] Running in pattern-matching mode (no API key or library missing)")
    
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
            query = request.query.strip()
            username = request.username
            
            print(f"[LLM] Query from {username}: {query}")
            
            if request.request_id:
                if request.request_id in self._recent_requests:
                    print(f"[LLM] 🧠 Duplicate request_id detected: {request.request_id} — skipping duplicate Gemini call.")
                    cached = self._cached_responses.get(request.request_id, "")
                    return llm_pb2.QueryResponse(
                        success=True,
                        response=cached or "(Duplicate query ignored to prevent redundant API call.)",
                        error=""
                    )
                self._recent_requests.add(request.request_id)
                self._cleanup_recent_requests() 

            # Use Gemini if available, otherwise pattern matching
            if self.use_gemini:
                response = self._query_gemini(query, username)
            else:
                response = self._process_query_pattern_matching(query.lower())
            
            if request.request_id:
                self._cached_responses[request.request_id] = response

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
    
    def _build_context_for_gemini(self):
        """Build rich context for Gemini from logs and inventory"""
        context = {
            "current_inventory": self.inventory,
            "total_products": len(self.inventory),
            "total_units": sum(self.inventory.values()),
            "analysis": self._generate_analytics()
        }
        return context
    
    def _generate_analytics(self):
        """Generate analytics from historical logs"""
        analytics = {
            "activity_summary": {},
            "product_trends": {},
            "user_activity": {},
            "temporal_patterns": {},
            "velocity_metrics": {}
        }
        
        if not self.logs:
            return analytics
        
        # Activity summary
        operations = defaultdict(int)
        for log in self.logs:
            operations[log['operation']] += 1
        analytics["activity_summary"] = dict(operations)
        
        # Product trends
        product_changes = defaultdict(list)
        for log in self.logs:
            product = log.get('product')
            timestamp = log.get('timestamp', '')
            
            if log['operation'] == 'add_inventory':
                change = log.get('qty_change', 0)
            else:
                change = log.get('new_qty', 0)
            
            product_changes[product].append({
                'timestamp': timestamp,
                'change': change,
                'operation': log['operation']
            })
        
        analytics["product_trends"] = dict(product_changes)
        
        # User activity
        user_actions = defaultdict(int)
        for log in self.logs:
            user = log.get('username', 'unknown')
            user_actions[user] += 1
        analytics["user_activity"] = dict(user_actions)
        
        # Temporal patterns (recent activity)
        now = datetime.now()
        recent_24h = []
        recent_7d = []
        
        for log in self.logs:
            try:
                timestamp_str = log.get('timestamp', '')
                if timestamp_str:
                    log_time = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                    hours_ago = (now - log_time.replace(tzinfo=None)).total_seconds() / 3600
                    
                    if hours_ago <= 24:
                        recent_24h.append(log)
                    if hours_ago <= 168:  # 7 days
                        recent_7d.append(log)
            except:
                continue
        
        analytics["temporal_patterns"] = {
            "last_24h_operations": len(recent_24h),
            "last_7d_operations": len(recent_7d)
        }
        
        # Velocity metrics (operations per day)
        if len(self.logs) >= 2:
            try:
                first_log_time = datetime.fromisoformat(self.logs[0]['timestamp'].replace('Z', '+00:00'))
                last_log_time = datetime.fromisoformat(self.logs[-1]['timestamp'].replace('Z', '+00:00'))
                days_span = max(1, (last_log_time - first_log_time).days)
                
                analytics["velocity_metrics"] = {
                    "operations_per_day": round(len(self.logs) / days_span, 2),
                    "days_tracked": days_span
                }
            except:
                pass
        
        return analytics
    

    def _cleanup_recent_requests(self):
        """Limit memory usage of recent request cache"""
        if len(self._recent_requests) > 100:
            self._recent_requests = set(list(self._recent_requests)[-50:])
            for rid in list(self._cached_responses.keys())[:-50]:
                self._cached_responses.pop(rid, None)
    
    def _query_gemini(self, query: str, username: str) -> str:
        """Process natural language query using Gemini LLM"""
        context = self._build_context_for_gemini()

        prompt = f"""
You are an AI inventory management assistant.
Use the following inventory data to answer user questions.

CURRENT INVENTORY:
{json.dumps(context['current_inventory'], indent=2)}

ANALYTICS SUMMARY:
{json.dumps(context['analysis'], indent=2)}

USER: {username}
QUESTION: {query}

INSTRUCTIONS:
- Provide concise, data-backed analysis.
- Predict demand or pricing trends when asked.
- Mention assumptions if data is limited.
"""

        print(f"[LLM] Sending REST request to Gemini ({len(prompt)} chars)")
        api_key = os.getenv("GEMINI_API_KEY")

        if not api_key:
            print("[LLM] ❌ GEMINI_API_KEY not set in environment.")
            return self._process_query_pattern_matching(query.lower())

        url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key={api_key}"
        headers = {"Content-Type": "application/json"}
        payload = {"contents": [{"parts": [{"text": prompt}]}]}

        try:
            response = requests.post(url, headers=headers, json=payload, timeout=20)
            if response.status_code != 200:
                print(f"[LLM] ❌ Gemini HTTP error {response.status_code}: {response.text}")
                return self._process_query_pattern_matching(query.lower())

            data = response.json()
            print(f"[LLM] ✅ Raw Gemini response: {data}")

        # Extract text from Gemini’s structured output
            text = ""
            if "candidates" in data and data["candidates"]:
                parts = data["candidates"][0].get("content", {}).get("parts", [])
                for p in parts:
                    text += p.get("text", "")

            if text.strip():
                print(f"[LLM] ✅ Gemini responded: {text[:300]}...")
                return f"🤖 AI Analysis (Gemini)\n{'='*60}\n{text}\n{'='*60}"
            else:
                print("[LLM] ⚠️ Gemini returned empty text field.")
                return self._process_query_pattern_matching(query.lower())

        except requests.exceptions.Timeout:
            print("[LLM] ⚠️ Gemini request timed out after 20s.")
            return self._process_query_pattern_matching(query.lower())
        except Exception as e:
            print(f"[LLM] ❌ Gemini request exception: {e}")
            return self._process_query_pattern_matching(query.lower())
    
    def _process_query_pattern_matching(self, query: str) -> str:
        """Process natural language query using simple pattern matching (FALLBACK)"""
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
                    response += f"  {user} added {change} {product}\n"
                else:
                    new_qty = entry.get('new_qty', 0)
                    response += f"  {user} updated {product} to {new_qty}\n"
                
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
            gemini_status = "✅ ENABLED" if self.use_gemini else "❌ DISABLED (set GEMINI_API_KEY)"
            
            return (
                f"🤖 AI Inventory Assistant - Available Queries:\n"
                + "="*60 + "\n"
                + f"AI Mode: {gemini_status}\n\n"
                + "📊 Inventory Info:\n"
                + "  • 'Show all inventory' or 'List everything'\n"
                + "  • 'How much [product] do we have?'\n"
                + "  • 'What's the total?'\n\n"
                + "⚠️ Stock Alerts:\n"
                + "  • 'What's running low?'\n"
                + "  • 'Show out of stock items'\n"
                + "  • 'Which product has the most stock?'\n\n"
                + "📈 Activity:\n"
                + "  • 'Show recent changes'\n"
                + "  • 'What's the latest activity?'\n\n"
                + "🔮 AI-Powered (requires Gemini API):\n"
                + "  • 'Predict demand for [product]'\n"
                + "  • 'Analyze cost trends'\n"
                + "  • 'What should I order next?'\n"
                + "  • 'Show me purchasing patterns'\n"
                + "  • 'Which products are most volatile?'\n"
                + "  • 'Forecast inventory needs for next week'\n"
                + "  • 'Identify slow-moving items'\n"
                + "="*60
            )
        
        # Default response - summarize current state
        total = sum(self.inventory.values())
        num_products = len(self.inventory)
        
        return (
            f"📦 You asked: '{query}'\n\n"
            f"Current Inventory Summary:\n"
            + "="*40 + "\n"
            + f"  • Total products: {num_products}\n"
            + f"  • Total units: {total}\n\n"
            + f"💡 Try asking:\n"
            + f"  • 'Show all inventory'\n"
            + f"  • 'How much [product] do we have?'\n"
            + f"  • 'What's running low?'\n"
            + f"  • 'Show recent changes'\n"
            + f"  • 'Predict demand for apples' (AI)\n"
            + f"  • 'help' for more options\n"
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