import grpc
from concurrent import futures
import llm_pb2
import llm_pb2_grpc
import json
import os
from datetime import datetime, timedelta
from collections import defaultdict, deque, OrderedDict
import requests
import time
import random
from dotenv import load_dotenv
import threading

# Load environment variables from .env file
load_dotenv()

# Gemini API
try:
    import google.generativeai as genai
    GEMINI_AVAILABLE = True
except ImportError:
    GEMINI_AVAILABLE = False
    print("[LLM] Warning: google-generativeai not installed. Run: pip install google-generativeai")


class LLMServicer(llm_pb2_grpc.LLMServiceServicer):
    def __init__(self):
        self.log_file = "/app/llm_data/llm_context_log.jsonl"
        self.logs = []
        self.inventory = {}
        # Thread-safety
        self._lock = threading.RLock()
        # Throttle outbound calls to Gemini (seconds between calls)
        self._min_interval = float(os.getenv("GEMINI_MIN_INTERVAL", "5.0"))
        self._last_call_ts = 0.0

        # Track recent request_ids to prevent duplicate processing (bounded, ordered)
        self._recent_ids = deque(maxlen=200)
        self._recent_set = set()
        self._cached_responses = OrderedDict()  # request_id -> response (bounded)
        self._text_cache = OrderedDict()
        self._text_cache_limit = 300

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
        
        if not api_key:
            print(f"[LLM] No GEMINI_API_KEY found in environment variables")
            print(f"[LLM] Running in pattern-matching mode")
            return
        
        if GEMINI_AVAILABLE:
            try:
                genai.configure(
                    api_key=api_key,
                    client_options={"api_endpoint": "https://generativelanguage.googleapis.com/v1/"}
                )
                # Use the correct model name
                self.gemini_model = genai.GenerativeModel('models/gemini-2.0-flash-exp')
                self.use_gemini = True
                print(f"[LLM] Gemini API initialized (model: gemini-2.0-flash-exp)")
            except Exception as e:
                print(f"[LLM] Gemini initialization failed: {e}")
                print(f"[LLM] Falling back to pattern-matching mode")
        else:
            print(f"[LLM] Running in pattern-matching mode")
    
    def _load_logs(self):
        """Load logs from file"""
        if not os.path.exists(self.log_file):
            return
        try:
            with self._lock:
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
            with self._lock:
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
            with self._lock:
                # Clear existing data
                self.logs = []
                self.inventory = {}
                # Rewrite log file atomically
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
            return llm_pb2.SyncLogsResponse(success=True, logs_synced=len(request.logs), message=f"Synced {len(request.logs)} logs")
        except Exception as e:
            print(f"[LLM] Sync error: {e}")
            return llm_pb2.SyncLogsResponse(success=False, logs_synced=0, message=f"Error: {str(e)}")

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
            with self._lock:
                self.logs.append(entry)
                self._apply_to_inventory(entry)
                self._persist_log(entry)
            print(f"[LLM] Appended log {entry['index']}: {entry['operation']} {entry['product']}")
            return llm_pb2.AppendLogResponse(success=True, message="Log appended")
        except Exception as e:
            return llm_pb2.AppendLogResponse(success=False, message=f"Error: {str(e)}")

    def QueryInventory(self, request, context):
        """Answer natural language queries about inventory"""
        try:
            query = request.query.strip()
            username = request.username
            req_id = getattr(request, "request_id", "") or ""

            print(f"[LLM] Query from {username}: {query}")

            # Duplicate detection
            if req_id:
                with self._lock:
                    if req_id in self._recent_set:
                        cached = self._cached_responses.get(req_id, "")
                        print(f"[LLM] Duplicate request_id detected: {req_id}")
                        return llm_pb2.QueryResponse(success=True, response=cached or "(Duplicate query - cached response)", error="")
                    self._recent_ids.append(req_id)
                    self._recent_set.add(req_id)
                    while len(self._recent_set) > len(self._recent_ids):
                        stale = next(iter(self._recent_set - set(self._recent_ids)))
                        self._recent_set.discard(stale)
                    while len(self._cached_responses) > 200:
                        self._cached_responses.popitem(last=False)

            # Build response
            if self.use_gemini:
                response = self._query_gemini(query, username)
            else:
                # snapshot inventory/logs under lock for consistency
                with self._lock:
                    response = self._process_query_pattern_matching(query.lower())

            # Cache response
            if req_id:
                with self._lock:
                    self._cached_responses[req_id] = response

            return llm_pb2.QueryResponse(success=True, response=response, error="")
        except Exception as e:
            print(f"[LLM] Query error: {e}")
            return llm_pb2.QueryResponse(success=False, response="", error=str(e))

    def _build_context_for_gemini(self):
        """Build rich context for Gemini from logs and inventory"""
        with self._lock:
            context = {
                "current_inventory": dict(self.inventory),
                "total_products": len(self.inventory),
                "total_units": sum(self.inventory.values()),
                "analysis": self._generate_analytics()
            }
        return context

    def _generate_analytics(self):
        """Generate analytics from historical logs"""
        with self._lock:
            logs_copy = list(self.logs)
        
        analytics = {
            "activity_summary": {},
            "product_trends": {},
            "user_activity": {},
            "temporal_patterns": {},
            "velocity_metrics": {}
        }
        
        if not logs_copy:
            return analytics
        
        # Activity summary
        operations = defaultdict(int)
        for log in logs_copy:
            operations[log['operation']] += 1
        analytics["activity_summary"] = dict(operations)
        
        # Product trends
        product_changes = defaultdict(list)
        for log in logs_copy:
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
        for log in logs_copy:
            user = log.get('username', 'unknown')
            user_actions[user] += 1
        analytics["user_activity"] = dict(user_actions)
        
        # Temporal patterns
        now = datetime.now()
        recent_24h = []
        recent_7d = []
        
        for log in logs_copy:
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
        
        # Velocity metrics
        if len(logs_copy) >= 2:
            try:
                first_log_time = datetime.fromisoformat(logs_copy[0]['timestamp'].replace('Z', '+00:00'))
                last_log_time = datetime.fromisoformat(logs_copy[-1]['timestamp'].replace('Z', '+00:00'))
                days_span = max(1, (last_log_time - first_log_time).days)
                
                analytics["velocity_metrics"] = {
                    "operations_per_day": round(len(logs_copy) / days_span, 2),
                    "days_tracked": days_span
                }
            except:
                pass
        
        return analytics
    
    def _query_gemini(self, query: str, username: str) -> str:
        """Send query to Gemini with retry logic and proper prompt"""
        api_key = os.getenv("GEMINI_API_KEY", "").strip()
        if not api_key:
            return self._process_query_pattern_matching(query.lower())

        # Check text cache
        normalized = query.strip().lower()
        with self._lock:
            cached_text = self._text_cache.get(normalized)
        if cached_text:
            return cached_text

        context = self._build_context_for_gemini()
        inventory_snapshot = json.dumps(context.get("current_inventory", {}), indent=2)
        analytics = json.dumps(context.get("analysis", {}), indent=2)

        # Optimized prompt format
        prompt = (
            "You are an AI assistant for inventory management.\n\n"
            f"CURRENT INVENTORY:\n{inventory_snapshot}\n\n"
            f"ANALYTICS:\n{analytics}\n\n"
            f"USER: {username}\n"
            f"QUESTION: {query}\n\n"
            "Provide a concise, data-backed answer with specific numbers and actionable insights."
        )

        url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash-exp:generateContent?key={api_key}"
        headers = {"Content-Type": "application/json"}
        payload = {"contents": [{"parts": [{"text": prompt}]}]}

        max_attempts = 3
        base_backoff = float(os.getenv("GEMINI_BASE_BACKOFF", "1.5"))

        for attempt in range(1, max_attempts + 1):
            # Throttle requests
            with self._lock:
                now = time.time()
                wait = self._min_interval - (now - self._last_call_ts)
            if wait > 0:
                time.sleep(wait)
            with self._lock:
                self._last_call_ts = time.time()

            print(f"[LLM] Sending Gemini request (attempt {attempt}/{max_attempts})")
            try:
                resp = requests.post(url, headers=headers, json=payload, timeout=60)
            except requests.exceptions.Timeout:
                backoff = base_backoff * (2 ** (attempt - 1))
                print(f"[LLM] Timeout, backoff {backoff:.1f}s")
                time.sleep(backoff)
                continue
            except Exception as e:
                print(f"[LLM] Unexpected error: {e}")
                break

            if resp.status_code == 200:
                data = resp.json()
                text = ""
                
                # Parse response with multiple fallbacks
                if isinstance(data, dict):
                    # Try candidates format
                    cands = data.get("candidates", [])
                    if cands:
                        parts = cands[0].get("content", {}).get("parts", [])
                        for part in parts:
                            if isinstance(part, dict):
                                text += part.get("text", "")
                    
                    # Try outputs format
                    if not text and "outputs" in data:
                        for out in data["outputs"]:
                            if isinstance(out, dict):
                                if "content" in out:
                                    for c in out["content"]:
                                        if isinstance(c, dict):
                                            text += c.get("text", "")
                                if "text" in out:
                                    text += out["text"]
                    
                    # Try output format
                    if not text and "output" in data:
                        text += data["output"].get("text", "")
                
                if text.strip():
                    answer = f"AI Response:\n{text.strip()}"
                    with self._lock:
                        self._text_cache[normalized] = answer
                        while len(self._text_cache) > self._text_cache_limit:
                            self._text_cache.popitem(last=False)
                    return answer
                
                print("[LLM] Empty response from Gemini")
            
            elif resp.status_code == 429:
                ra = resp.headers.get("Retry-After")
                if ra:
                    try:
                        sleep_s = float(ra)
                        print(f"[LLM] 429 Retry-After {sleep_s}s")
                        time.sleep(sleep_s)
                        continue
                    except:
                        pass
                backoff = base_backoff * (2 ** (attempt - 1)) + random.uniform(0, 1)
                print(f"[LLM] 429 rate limit, backing off {backoff:.1f}s")
                time.sleep(backoff)
                continue
            
            elif resp.status_code in (500, 502, 503, 504):
                backoff = base_backoff * (2 ** (attempt - 1)) + random.uniform(0, 1)
                print(f"[LLM] {resp.status_code} server error, backoff {backoff:.1f}s")
                time.sleep(backoff)
                continue
            
            else:
                print(f"[LLM] Gemini HTTP {resp.status_code}: {resp.text[:200]}")
                break

        print("[LLM] Falling back to pattern matching")
        return self._process_query_pattern_matching(normalized)
    
    def _process_query_pattern_matching(self, query: str) -> str:
        """Process query using pattern matching (fallback)"""
        query_lower = query.lower()
        
        # List all inventory
        if any(word in query_lower for word in ["list", "show all", "what do we have", "inventory", "everything"]):
            if not self.inventory:
                return "The inventory is currently empty."
            
            response = "Current Inventory:\n" + "="*50 + "\n"
            for product, qty in sorted(self.inventory.items()):
                response += f"  • {product}: {qty} units\n"
            response += "="*50
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
        
        # Low stock
        if any(word in query_lower for word in ["low stock", "running low", "shortage", "need to order"]):
            low_stock = {p: q for p, q in self.inventory.items() if q < 10}
            if not low_stock:
                return "No products are running low on stock (all ≥10 units)."
            
            response = "Low Stock Alert (< 10 units):\n" + "="*50 + "\n"
            for product, qty in sorted(low_stock.items()):
                response += f"  • {product}: {qty} units\n"
            response += "="*50
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
            
            recent = self.logs[-5:]
            response = "Recent Inventory Changes:\n" + "="*50 + "\n"
            for entry in reversed(recent):
                product = entry['product']
                user = entry.get('username', 'unknown')
                timestamp = entry.get('timestamp', '')
                
                if entry['operation'] == 'add_inventory':
                    change = entry.get('qty_change', 0)
                    response += f" {user} added {change} {product}\n"
                else:
                    new_qty = entry.get('new_qty', 0)
                    response += f"  {user} updated {product} to {new_qty}\n"
                
                if timestamp:
                    response += f"      ({timestamp[:19]})\n"
            response += "="*50
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
            gemini_status = "ENABLED" if self.use_gemini else "DISABLED (using pattern matching)"
            return (
                f"AI Inventory Assistant\n"
                f"AI Mode: {gemini_status}\n"
                + "="*60 + "\n"
                + "Inventory Info:\n"
                + "  • 'Show all inventory'\n"
                + "  • 'How much [product] do we have?'\n"
                + "  • 'What's the total?'\n\n"
                + "Stock Alerts:\n"
                + "  • 'What's running low?'\n"
                + "  • 'Show out of stock items'\n"
                + "  • 'Which product has the most stock?'\n\n"
                + "Activity:\n"
                + "  • 'Show recent changes'\n\n"
                + "AI-Powered (Gemini):\n"
                + "  • 'Predict demand for [product]'\n"
                + "  • 'Analyze purchasing patterns'\n"
                + "  • 'What should I order next?'\n"
                + "="*60
            )
        
        # Default
        total = sum(self.inventory.values())
        num_products = len(self.inventory)
        
        return (
            f" You asked: '{query}'\n\n"
            f"Current Summary:\n"
            + "="*50 + "\n"
            + f"  • Products: {num_products}\n"
            + f"  • Total units: {total}\n\n"
            + "Try:\n"
            + "  • 'show all inventory'\n"
            + "  • 'how much [product] do we have?'\n"
            + "  • 'what's running low?'\n"
            + "  • 'show recent changes'\n"
            + "  • 'help' for more options\n"
            + "="*50
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