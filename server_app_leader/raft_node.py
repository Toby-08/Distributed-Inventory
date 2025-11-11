import os
import json
import threading
import time
import random
import grpc
from enum import Enum
from datetime import datetime, timezone
from typing import Optional, Dict, List

import raft_pb2
import raft_pb2_grpc
import llm_pb2
import llm_pb2_grpc


class NodeState(Enum):
    FOLLOWER = 1
    CANDIDATE = 2
    LEADER = 3

class RaftNode:
    def __init__(self, node_id: str, peers: list, port: int, llm_server_address: str = "127.0.0.1:50054"):
        self.node_id = node_id
        self.peers = peers
        self.port = port
        self.llm_server_address = llm_server_address
        self.leader_id = None

        # Storage paths - EACH NODE MUST HAVE ITS OWN DIRECTORY
        self.data_dir = os.path.join("server_data", self.node_id)
        self.log_path = os.path.join(self.data_dir, "raft_log.jsonl")
        self.state_path = os.path.join(self.data_dir, "state.json")
        
        # Create directory if it doesn't exist
        os.makedirs(self.data_dir, exist_ok=True)
        
        print(f"[{self.node_id}] Data directory: {self.data_dir}")
        print(f"[{self.node_id}] Log path: {self.log_path}")
        
        self.log = []
        self.inventory = {}
        self.current_term = 0
        self.voted_for = None
        self.commit_index = 0
        self.last_applied = 0

        # Load persisted state and log BEFORE starting threads
        self._load_state()
        self._load_log()
        
        print(f"[{self.node_id}] Loaded {len(self.log)} log entries (term={self.current_term}, voted_for={self.voted_for})")
        
        # Debug: show inventory after loading
        if self.inventory:
            print(f"[{self.node_id}] Current inventory: {self.inventory}")
        
        # Missing inits
        self.state_lock = threading.RLock()
        self.request_cache_lock = threading.Lock()
        self.completed_requests: Dict[str, dict] = {}
        self.max_cache_size = 1000

        self.state = NodeState.FOLLOWER
        self.running = False

        # Timers/intervals
        self.heartbeat_interval = 0.10  # 100ms heartbeats
        self.health_check_interval = 1.0
        self.dead_peer_threshold = 3.0
        self.last_heartbeat = time.time()
        self.election_timeout = self._random_election_timeout()

        # Peer health and replication tracking
        self.dead_peers = set()
        self.peer_last_seen: Dict[str, float] = {p: time.time() for p in self.peers}
        self.next_index: Dict[str, int] = {p: len(self.log) + 1 for p in self.peers}
        self.match_index: Dict[str, int] = {p: 0 for p in self.peers}

        # LLM tracking
        self.llm_server_address = llm_server_address
        self.llm_available = False
        self.llm_last_check = 0.0
        self.llm_check_interval = 5.0  # Check every 5 seconds
        self.llm_consecutive_failures = 0
        self.last_llm_synced_term = 0
        self.last_llm_synced_index = 0

        self.failed_elections = 0
    
    def _random_election_timeout(self) -> float:
        """Random election timeout with backoff on failures"""
        base_timeout = random.uniform(1.5, 3.0)  
        
        # Add backoff based on failed elections
        if hasattr(self, 'failed_elections'):
            backoff = min(self.failed_elections * 0.5, 3.0)  # Max 3s backoff
            base_timeout += backoff
    
        return base_timeout
    
    def _load_state(self):
        """Load current_term and voted_for from state.json."""
        if not os.path.exists(self.state_path):
            return
        try:
            with open(self.state_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            self.current_term = int(data.get("current_term", 0))
            self.voted_for = data.get("voted_for")
        except Exception as e:
            print(f"[{self.node_id}] Failed to load state: {e}")

    def _persist_state(self):
        """Persist current_term and voted_for."""
        try:
            tmp_path = self.state_path + ".tmp"
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump({
                    "current_term": self.current_term,
                    "voted_for": self.voted_for
                }, f)
            os.replace(tmp_path, self.state_path)
        except Exception as e:
            print(f"[{self.node_id}] Failed to persist state: {e}")

    def _load_log(self):
        """Load log entries from raft_log.jsonl."""
        if not os.path.exists(self.log_path):
            print(f"[{self.node_id}] No existing log file at {self.log_path}")
            return
        
        try:
            loaded_count = 0
            with open(self.log_path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        entry = json.loads(line)
                     
                        if "term" in entry and "index" in entry:
                            self.log.append(entry)
                            # Reconstruct inventory by applying
                            self._apply_loaded(entry)
                            loaded_count += 1
                    except json.JSONDecodeError as e:
                        print(f"[{self.node_id}] Skipping malformed log line: {e}")
                        continue
            
            # Rebuild commit pointers
            self.commit_index = len(self.log)
            self.last_applied = len(self.log)
            
            print(f"[{self.node_id}] Successfully loaded {loaded_count} log entries from {self.log_path}")
            
        except Exception as e:
            print(f"[{self.node_id}] Failed to load log: {e}")
            import traceback
            traceback.print_exc()

    def _apply_loaded(self, entry: dict):
        """Apply an entry during load (no LLM calls)."""
        op = entry.get("operation")
        product = entry.get("product")
        if op == "add_inventory":
            old = self.inventory.get(product, 0)
            self.inventory[product] = old + entry.get("qty_change", 0)
        elif op == "update_inventory":
            self.inventory[product] = entry.get("new_qty", 0)

    def _persist_log_entry(self, entry: dict):
        """Append one entry to log file safely."""
        try:
            with open(self.log_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(entry) + "\n")
                f.flush()
                os.fsync(f.fileno())
        except Exception as e:
            print(f"[{self.node_id}] Failed to persist log entry: {e}")

    # Health tracking methods
    def _mark_peer_dead(self, peer: str):
        """Mark a peer as dead"""
        with self.state_lock:
            if peer not in self.dead_peers:
                print(f"[{self.node_id}] Marking {peer} as DEAD (unreachable)")
                self.dead_peers.add(peer)
    
    def _mark_peer_alive(self, peer: str):
        """Mark a peer as alive again"""
        with self.state_lock:
            if peer in self.dead_peers:
                print(f"[{self.node_id}] {peer} is BACK ONLINE")
                self.dead_peers.remove(peer)
            self.peer_last_seen[peer] = time.time()
    
    def _get_live_peers(self) -> list:
        """Get list of currently alive peers"""
        return [p for p in self.peers if p not in self.dead_peers]
    
    def _calculate_majority(self) -> int:
        """Fixed majority based on full cluster size (peers + self)"""
        total = len(self.peers) + 1
        return (total // 2) + 1
    
    def _can_reach_quorum(self, timeout: float = 0.2) -> bool:
        """Quick connectivity probe to avoid elections from a minority partition."""
        majority = self._calculate_majority()
        reachable = 1  # self
        for peer in self.peers:
            try:
                ch = grpc.insecure_channel(peer)
                grpc.channel_ready_future(ch).result(timeout=timeout)
                ch.close()
                reachable += 1
            except Exception:
                pass
        return reachable >= majority

    def _health_check_loop(self):
        """Background thread to check peer health"""
        while self.running:
            time.sleep(self.health_check_interval)
            
            for peer in self.peers:
                try:
                    channel = grpc.insecure_channel(peer)
                    # Quick connectivity check
                    grpc.channel_ready_future(channel).result(timeout=0.2)
                    self._mark_peer_alive(peer)
                    channel.close()
                except:
                    # Check if peer has been unreachable for too long
                    with self.state_lock:
                        last_seen = self.peer_last_seen.get(peer, time.time())
                        if time.time() - last_seen > self.dead_peer_threshold:
                            self._mark_peer_dead(peer)
    
    def _become_leader(self):
        print(f"[{self.node_id}] *** Became LEADER for term {self.current_term}")
        self.state = NodeState.LEADER
        self.failed_elections = 0  # Reset failed election counter
        
        for peer in self.peers:
            self.next_index[peer] = len(self.log) + 1
            self.match_index[peer] = 0

        def sync_with_llm():
            try:
                self.llm_last_check = 0
                # Re-check role (could have stepped down)
                if self.state != NodeState.LEADER:
                    return
                if not self._check_llm_health():
                    print(f"[{self.node_id}] LLM unavailable - will auto-sync later")
                    return
                self._bulk_sync_llm_if_needed()
            except Exception as e:
                print(f"[{self.node_id}] LLM sync failed (non-critical): {e}")

        threading.Thread(target=sync_with_llm, daemon=True).start()
        self._send_heartbeats()

    def _bulk_sync_llm_if_needed(self):
        """Leader-only bulk sync with dedupe."""
        with self.state_lock:
            if self.state != NodeState.LEADER:
                print(f"[{self.node_id}] Not leader, skipping sync")
                return
            current_index = len(self.log)
            if (self.last_llm_synced_term, self.last_llm_synced_index) == (self.current_term, current_index):
                print(f"[{self.node_id}] Already synced term={self.current_term} index={current_index}")
                return
        
        # Proceed outside the lock
        print(f"[{self.node_id}] LLM bulk sync: term={self.current_term} logs={current_index}")
        ok = self._sync_logs_to_llm()
        if ok:
            with self.state_lock:
                self.last_llm_synced_term = self.current_term
                self.last_llm_synced_index = len(self.log)
        
        print(f"[{self.node_id}] LLM re-sync completed at index {self.last_llm_synced_index}")

    def _sync_logs_to_llm(self) -> bool:
        """Bulk sync all logs to LLM server. Returns True if successful."""
        print(f"[{self.node_id}] _sync_logs_to_llm called with {len(self.log)} logs")
        
        try:
            print(f"[{self.node_id}] Connecting to LLM at {self.llm_server_address}...")
            
            channel = grpc.insecure_channel(
                self.llm_server_address,
                options=[('grpc.enable_retries', 0)]
            )
            
            # Test connection first
            try:
                grpc.channel_ready_future(channel).result(timeout=2.0)
                print(f"[{self.node_id}] LLM connection established")
            except grpc.FutureTimeoutError:
                print(f"[{self.node_id}] LLM server unreachable (timeout)")
                channel.close()
                return False
            
            stub = llm_pb2_grpc.LLMServiceStub(channel)
            
            # Convert logs to protobuf format
            log_entries = []
            for entry in self.log:
                log_entries.append(llm_pb2.LogEntry(
                    term=entry['term'],
                    index=entry['index'],
                    operation=entry['operation'],
                    product=entry['product'],
                    qty_change=entry.get('qty_change', 0),
                    new_qty=entry.get('new_qty', 0),
                    username=entry.get('username', ''),
                    timestamp=entry.get('timestamp', ''),
                    request_id=entry.get('request_id', '')
                ))
            
            print(f"[{self.node_id}] Sending {len(log_entries)} logs to LLM...")
            
            # Call SyncLogs RPC
            response = stub.SyncLogs(llm_pb2.SyncLogsRequest(
                leader_id=self.node_id,
                term=self.current_term,
                logs=log_entries
            ), timeout=15.0)
            
            channel.close()
            
            if response.success:
                print(f"[{self.node_id}] LLM acknowledged {response.logs_synced} logs: {response.message}")
                return True
            else:
                print(f"[{self.node_id}] LLM sync failed: {response.message}")
                return False
                
        except grpc.RpcError as e:
            print(f"[{self.node_id}] LLM RPC error: {e.code()} - {e.details()}")
            import traceback
            traceback.print_exc()
            return False
        except Exception as e:
            print(f"[{self.node_id}] LLM sync exception: {type(e).__name__}: {e}")
            import traceback
            traceback.print_exc()
            return False

    def _resync_llm_on_recovery(self):
        """Re-sync all logs to LLM when it comes back online"""
        try:
            # Re-check leadership before syncing
            with self.state_lock:
                if self.state != NodeState.LEADER:
                    return
            
            time.sleep(0.5)  # Small delay to let LLM fully start
            self._bulk_sync_llm_if_needed()
            
        except Exception as e:
            print(f"[{self.node_id}] LLM re-sync failed: {e}")
            import traceback
            traceback.print_exc()

    def _send_log_to_llm(self, entry: dict):
        """Send a single log entry to LLM (for real-time append)"""
        # Leader-only lightweight append
        if self.state != NodeState.LEADER or not self.llm_available:
            return
        
        try:
            channel = grpc.insecure_channel(self.llm_server_address)
            grpc.channel_ready_future(channel).result(timeout=0.5)
            stub = llm_pb2_grpc.LLMServiceStub(channel)
            
            log_entry = llm_pb2.LogEntry(
                term=entry['term'],
                index=entry['index'],
                operation=entry['operation'],
                product=entry['product'],
                qty_change=entry.get('qty_change', 0),
                new_qty=entry.get('new_qty', 0),
                username=entry.get('username', ''),
                timestamp=entry.get('timestamp', ''),
                request_id=entry.get('request_id', '')
            )
            
            stub.AppendLog(llm_pb2.AppendLogRequest(
                log=log_entry,
                leader_id=self.node_id
            ), timeout=3.0)
            
            channel.close()
            print(f"[{self.node_id}] LLM received log {entry['index']}")
            
        except Exception:
            # Silent failure; health checker will adjust availability
            self.llm_available = False
    
    def start(self):
        """Start Raft consensus threads"""
        self.running = True
        
        # Election timer
        self.election_thread = threading.Thread(target=self._election_timer, daemon=True)
        self.election_thread.start()
        
        # Heartbeat timer
        self.heartbeat_thread = threading.Thread(target=self._heartbeat_timer, daemon=True)
        self.heartbeat_thread.start()
        
        # Health check for peers
        self.health_thread = threading.Thread(target=self._health_check_loop, daemon=True)
        self.health_thread.start()
        
        # LLM health monitor
        self.llm_health_thread = threading.Thread(target=self._llm_health_monitor, daemon=True)
        self.llm_health_thread.start()
        
        print(f"[{self.node_id}] Raft consensus engine started")

    def _election_timer(self):
        """Election timeout loop"""
        while self.running:
            time.sleep(0.01)
            start_election = False
            with self.state_lock:
                if self.state != NodeState.LEADER:
                    elapsed = time.time() - self.last_heartbeat
                    if elapsed > self.election_timeout:
                        # Only proceed if we can likely reach quorum
                        if not self._can_reach_quorum(timeout=0.25):
                            # Backoff and try later; do NOT bump term
                            self.failed_elections = min(self.failed_elections + 1, 10)
                            backoff = self._random_election_timeout() * (1.5 + 0.5 * self.failed_elections)
                            self.election_timeout = backoff
                            self.last_heartbeat = time.time()
                            print(f"[{self.node_id}] Skipping election (no quorum). Backing off {backoff:.2f}s")
                        else:
                            print(f"[{self.node_id}] Election timeout! Starting election...")
                            start_election = True
            if start_election:
                self._start_election()

    def _start_election(self):
        """Start a new election"""
        with self.state_lock:
            # Only start election if we're a follower/candidate and haven't received recent heartbeat
            current_time = time.time()
            
            # Don't start election if we recently received a heartbeat from a valid leader
            if current_time - self.last_heartbeat < self.election_timeout:
                return
            
            # Increment term and become candidate
            self.current_term += 1
            self.state = NodeState.CANDIDATE
            self.voted_for = self.node_id  # Vote for self
            self._persist_state()
            
            # Reset election timeout with jitter to prevent split votes
            self.election_timeout = self._random_election_timeout()
            self.last_heartbeat = current_time  # Reset timer
            
            current_term = self.current_term
            votes_received = 1  # Vote for self
            votes_needed = self._calculate_majority()
            
            print(f"[{self.node_id}] STARTING ELECTION for term {current_term}")
        
        # Collect votes from peers
        vote_results = []
        
        # Request votes from all peers in parallel
        for peer in self.peers:
            thread = threading.Thread(
                target=lambda p: vote_results.append((p, self._request_vote_sync(p, current_term))),
                args=(peer,),
                daemon=True
            )
            thread.start()
        
        # Wait for vote responses (with timeout)
        time.sleep(1.0)  # Wait up to 1 second for votes
        
        # Count votes
        with self.state_lock:
            # Check if we're still a candidate in the same term
            if self.state != NodeState.CANDIDATE or self.current_term != current_term:
                print(f"[{self.node_id}] Election aborted (state/term changed)")
                return
            
            # Count granted votes
            for peer, vote_granted in vote_results:
                if vote_granted:
                    votes_received += 1
                    print(f"[{self.node_id}] Received vote from {peer} ({votes_received}/{votes_needed})")
            
            # Check if we won
            if votes_received >= votes_needed:
                print(f"[{self.node_id}] WON ELECTION with {votes_received}/{votes_needed} votes!")
                self._become_leader()
                return
            
            # Election failed
            # Check if we've received a heartbeat from a leader in the meantime
            if time.time() - self.last_heartbeat < 1.0:
                print(f"[{self.node_id}] Election failed, but valid leader exists. Stepping down.")
                self.state = NodeState.FOLLOWER
            else:
                print(f"[{self.node_id}] Election failed with {votes_received}/{votes_needed} votes")
                # Revert to follower to retry on next timeout
                self.state = NodeState.FOLLOWER

    def _request_vote_sync(self, peer: str, term: int) -> bool:
        """Synchronous vote request (for use in threads)"""
        try:
            channel = grpc.insecure_channel(peer)
            stub = raft_pb2_grpc.RaftServiceStub(channel)
            
            with self.state_lock:
                last_log_index = len(self.log)
                last_log_term = self.log[-1]['term'] if self.log else 0
            
            request = raft_pb2.VoteRequest(
                term=term,
                candidateId=self.node_id,
                lastLogIndex=last_log_index,
                lastLogTerm=last_log_term
            )
            
            response = stub.RequestVote(request, timeout=0.5)
            
            channel.close()
            
            # Mark peer alive on successful response
            self._mark_peer_alive(peer)
            
            with self.state_lock:
                # If we discover higher term, step down
                if response.term > self.current_term:
                    print(f"[{self.node_id}] Discovered higher term {response.term} from {peer}, stepping down")
                    self._revert_to_follower(response.term)
                    return False
        
            return response.voteGranted
        
        except grpc.RpcError as e:
            print(f"[{self.node_id}] RPC error from {peer}: {e.code()}")
            return False
        except Exception as e:
            print(f"[{self.node_id}] Error requesting vote from {peer}: {e}")
            return False

    def _bulk_sync_llm_if_needed(self):
        """Leader-only bulk sync with dedupe."""
        with self.state_lock:
            if self.state != NodeState.LEADER:
                print(f"[{self.node_id}] Not leader, skipping sync")
                return
            current_index = len(self.log)
            if (self.last_llm_synced_term, self.last_llm_synced_index) == (self.current_term, current_index):
                print(f"[{self.node_id}] Already synced term={self.current_term} index={current_index}")
                return
        
        # Proceed outside the lock
        print(f"[{self.node_id}] LLM bulk sync: term={self.current_term} logs={current_index}")
        ok = self._sync_logs_to_llm()
        if ok:
            with self.state_lock:
                self.last_llm_synced_term = self.current_term
                self.last_llm_synced_index = len(self.log)

    def _resync_llm_on_recovery(self):
        """Triggered when LLM becomes available."""
        try:
            # Re-check leadership before syncing
            if self.state != NodeState.LEADER:
                return
            self._bulk_sync_llm_if_needed()
        except Exception as e:
            print(f"[{self.node_id}] LLM re-sync failed: {e}")

    def _check_llm_health_direct(self) -> bool:
        """Direct LLM health check without caching"""
        try:
            channel = grpc.insecure_channel(
                self.llm_server_address,
                options=[('grpc.enable_retries', 0)]
            )
            grpc.channel_ready_future(channel).result(timeout=1.0)
            channel.close()
            
            if not self.llm_available:
                self.llm_consecutive_failures = 0
                self.llm_check_interval = 5.0  # Reset to normal interval
            
            self.llm_available = True
            return True
            
        except Exception:
            self.llm_available = False
            self.llm_consecutive_failures += 1
            
            # Exponential backoff (max 60s)
            self.llm_check_interval = min(5.0 * (1.5 ** min(self.llm_consecutive_failures, 5)), 60.0)
            
            return False

    def _check_llm_health(self) -> bool:
        """Cached LLM health check (for use in hot paths like log application)"""
        now = time.time()
        
        # Use cached result if recent
        if now - self.llm_last_check < 2.0:  # Cache for 2 seconds
            return self.llm_available
        
        self.llm_last_check = now
        return self._check_llm_health_direct()
    
    def is_leader(self) -> bool:
        """Check if this node is the leader"""
        with self.state_lock:
            return self.state == NodeState.LEADER
    
    def append_log_entry(self, operation: str, product: str, qty_change: int, 
                         new_qty: int, username: str, request_id: str) -> bool:
        with self.state_lock:
            if self.state != NodeState.LEADER:
                return False
            entry = {
                "term": self.current_term,
                "index": len(self.log) + 1,
                "operation": operation,
                "product": product,
                "qty_change": qty_change,
                "new_qty": new_qty,
                "username": username,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "request_id": request_id
            }
            self.log.append(entry)
            self._persist_log_entry(entry)
            self._persist_state()
            print(f"[{self.node_id}] Append log {entry['index']} op={operation} product={product}")
        self._send_heartbeats()
        return True
    
    def check_duplicate_request(self, request_id: str) -> Optional[dict]:
        """Check if request was already processed"""
        with self.request_cache_lock:
            return self.completed_requests.get(request_id)
    
    def cache_request_result(self, request_id: str, result: dict):
        """Cache request result for idempotency"""
        with self.request_cache_lock:
            self.completed_requests[request_id] = result
            
            # Limit cache size (simple FIFO)
            if len(self.completed_requests) > self.max_cache_size:
                oldest = next(iter(self.completed_requests))
                del self.completed_requests[oldest]
    
    def get_leader_address(self):
        """Return address of current leader if known"""
        if self.state == NodeState.LEADER:
            # Return own address
            return f"127.0.0.1:{self.port}"
        
        # Return voted_for or None
        # In real implementation, track leader from AppendEntries
        return None  # Could track last known leader here

    def _revert_to_follower(self, new_term: int):
        """Step down to follower state"""
        with self.state_lock:
            old_term = self.current_term
            old_state = self.state
        
            self.state = NodeState.FOLLOWER
            self.current_term = new_term
            self.voted_for = None  # Reset vote for new term
            self.last_heartbeat = time.time()  # Reset election timer
            self._persist_state()
        
        if old_state != NodeState.FOLLOWER or old_term != new_term:
            print(f"[{self.node_id}] Stepped down to FOLLOWER (term {old_term} → {new_term})")

    def _heartbeat_timer(self):
        """Periodically send heartbeats/log replication when leader."""
        while self.running:
            time.sleep(self.heartbeat_interval)
            try:
                with self.state_lock:
                    is_leader = (self.state == NodeState.LEADER)
                
                if is_leader:
                    # Debug: verify this runs
                    if not hasattr(self, '_heartbeat_send_count'):
                        self._heartbeat_send_count = 0
                    self._heartbeat_send_count += 1
                    
                    # Log every 70th heartbeat (~7 seconds at 100ms interval)
                    if self._heartbeat_send_count % 70 == 1:
                        print(f"[{self.node_id}] Sending heartbeats (count={self._heartbeat_send_count})...")
                    
                    self._send_heartbeats()
                    
            except Exception as e:
                print(f"[{self.node_id}] Heartbeat loop error: {e}")
                import traceback
                traceback.print_exc()

    def _send_heartbeats(self):
        """Send heartbeats to all followers"""
        with self.state_lock:
            if self.state != NodeState.LEADER:
                return
            peers = list(self.peers)
            current_term = self.current_term
            commit_index = self.commit_index
        
        # Send to each peer in parallel
        for peer in peers:
            threading.Thread(
                target=self._send_heartbeat_to_peer,
                args=(peer, current_term, commit_index),
                daemon=True
            ).start()

    def _send_heartbeat_to_peer(self, peer: str, term: int, commit_index: int):
        """Send AppendEntries RPC (heartbeat or log replication) to one peer"""
        try:
            channel = grpc.insecure_channel(peer)
            stub = raft_pb2_grpc.RaftServiceStub(channel)
            
            # Get prev log info and entries to send
            with self.state_lock:
                next_idx = self.next_index.get(peer, len(self.log) + 1)
                prev_log_index = next_idx - 1
                prev_log_term = self.log[prev_log_index - 1]['term'] if prev_log_index > 0 else 0
                
                # Get entries to replicate (from next_index onwards)
                entries_to_send = []
                if next_idx <= len(self.log):
                    for entry in self.log[next_idx - 1:]:
                        entries_to_send.append(raft_pb2.LogEntry(
                            term=entry['term'],
                            index=entry['index'],
                            operation=entry['operation'],
                            product=entry['product'],
                            qty_change=entry.get('qty_change', 0),
                            new_qty=entry.get('new_qty', 0),
                            username=entry.get('username', ''),
                            timestamp=entry.get('timestamp', ''),
                            request_id=entry.get('request_id', '')
                        ))
        
        # Build request
            request = raft_pb2.AppendEntriesRequest(
                term=term,
                leaderId=self.node_id,
                prevLogIndex=prev_log_index,
                prevLogTerm=prev_log_term,
                entries=entries_to_send,
                leaderCommit=commit_index
            )
            
            # Send RPC
            response = stub.AppendEntries(request, timeout=0.5)
            
            # ✅ ADD THIS: Mark peer alive on successful RPC
            self._mark_peer_alive(peer)
            
            # Handle response
            with self.state_lock:
                # If follower has higher term, step down
                if response.term > self.current_term:
                    print(f"[{self.node_id}] Stepping down: follower has higher term {response.term}")
                    self._revert_to_follower(response.term)
                    channel.close()
                    return
                
                # If successful, update next_index and match_index
                if response.success:
                    if entries_to_send:
                        # Log replication succeeded
                        new_match_index = prev_log_index + len(entries_to_send)
                        self.next_index[peer] = new_match_index + 1
                        self.match_index[peer] = new_match_index
                        
                        print(f"[{self.node_id}] Replicated {len(entries_to_send)} entries to {peer} (match_index={new_match_index})")
                        
                        # Update commit index if majority replicated
                        self._update_commit_index()
                else:
                    # Log inconsistency detected
                    # Track consecutive failures for this peer
                    if not hasattr(self, '_conflict_count'):
                        self._conflict_count = {}
                    
                    if peer not in self._conflict_count:
                        self._conflict_count[peer] = 0
                    
                    self._conflict_count[peer] += 1
                    
                    # Use binary search if we've had multiple consecutive conflicts
                    # This indicates a large gap in logs
                    if self._conflict_count[peer] >= 3:
                        print(f"[{self.node_id}] Multiple conflicts with {peer}, using binary search")
                        
                        # Perform binary search
                        new_next_idx = self._find_conflict_index_binary(peer, stub, term)
                        
                        if new_next_idx == -1:
                            # Step down was signaled
                            channel.close()
                            return
                        
                        self.next_index[peer] = new_next_idx
                        self._conflict_count[peer] = 0  # Reset counter
                        print(f"[{self.node_id}] Binary search set next_index[{peer}] = {new_next_idx}")
                    else:
                        # Use simple linear decrement for first few conflicts
                        self.next_index[peer] = max(1, self.next_index[peer] - 1)
                        print(f"[{self.node_id}] Log inconsistency with {peer}, decrementing next_index to {self.next_index[peer]}")
                
                channel.close()
        
        except grpc.RpcError as e:
            # ❌ Failed RPC - don't mark alive
            if not hasattr(self, '_heartbeat_errors'):
                self._heartbeat_errors = {}
            
            if peer not in self._heartbeat_errors:
                self._heartbeat_errors[peer] = 0
            
            self._heartbeat_errors[peer] += 1
            
            if self._heartbeat_errors[peer] <= 3:
                print(f"[{self.node_id}] Failed to send to {peer}: {e.code()}")
            
        except Exception as e:
            print(f"[{self.node_id}] Exception sending to {peer}: {e}")

    def _llm_health_monitor(self):
        """Background thread to monitor LLM server availability"""
        # Check immediately on first run (don't sleep first)
        first_check = True
        
        while self.running:
            # Only leaders need to sync with LLM
            with self.state_lock:
                is_leader = (self.state == NodeState.LEADER)
            
            if not is_leader:
                # Not leader - sleep and retry
                time.sleep(self.llm_check_interval)
                first_check = True  # Reset for when we become leader
                continue
            
            try:
                # On first check as leader, don't sleep
                if not first_check:
                    time.sleep(self.llm_check_interval)
                first_check = False
                
                # Check LLM health
                was_available = self.llm_available
                now_available = self._check_llm_health_direct()
                
                # LLM came back online - trigger sync
                if not was_available and now_available:
                    print(f"[{self.node_id}] LLM server RECOVERED! Syncing logs...")
                    threading.Thread(target=self._resync_llm_on_recovery, daemon=True).start()
                
                # LLM went offline
                elif was_available and not now_available:
                    print(f"[{self.node_id}] LLM server became UNAVAILABLE")
                
            except Exception as e:
                print(f"[{self.node_id}] LLM health monitor error: {e}")
    
    def _update_commit_index(self):
        """Update commit index based on majority replication"""
        with self.state_lock:
            if self.state != NodeState.LEADER:
                return
            
            # Find highest index replicated on majority
            # Count self + peers that have replicated each index
            for n in range(self.commit_index + 1, len(self.log) + 1):
                # Check if entry is from current term
                if n > len(self.log) or self.log[n - 1]['term'] != self.current_term:
                    continue
                
                # Count replicas (self + followers with match_index >= n)
                replicas = 1  # self
                for peer in self.peers:
                    if self.match_index.get(peer, 0) >= n:
                        replicas += 1
                
                # Check if majority
                majority = self._calculate_majority()
                if replicas >= majority:
                    old_commit = self.commit_index
                    self.commit_index = n
                    if old_commit != n:
                        print(f"[{self.node_id}] Committed log {n} (replicas={replicas}/{len(self.peers)+1})")
                else:
                    break  # Can't commit higher indices if this one isn't committed

    def _find_conflict_index_binary(self, peer: str, stub: raft_pb2_grpc.RaftServiceStub, term: int) -> int:
        """
        Use binary search to find the highest index where logs match between leader and follower.
        Returns the index to start replicating from.
        """
        with self.state_lock:
            if len(self.log) == 0:
                return 1
            
            left = 1
            right = len(self.log)
            last_matching = 0
        
        print(f"[{self.node_id}] Binary search for conflict with {peer} (range: {left}-{right})")
        
        while left <= right:
            mid = (left + right) // 2
            
            with self.state_lock:
                if mid > len(self.log):
                    right = mid - 1
                    continue
                    
                prev_log_index = mid
                prev_log_term = self.log[prev_log_index - 1]['term']
            
            try:
                # Send empty AppendEntries to probe if logs match at this index
                request = raft_pb2.AppendEntriesRequest(
                    term=term,
                    leaderId=self.node_id,
                    prevLogIndex=prev_log_index,
                    prevLogTerm=prev_log_term,
                    entries=[],  # Empty probe
                    leaderCommit=0
                )
                
                response = stub.AppendEntries(request, timeout=0.5)
                
                with self.state_lock:
                    if response.term > self.current_term:
                        # Follower has higher term, we need to step down
                        return -1  # Signal to abort
                
                if response.success:
                    # Logs match up to mid, try higher
                    last_matching = mid
                    left = mid + 1
                    print(f"[{self.node_id}]   Probe {mid}: MATCH, searching higher")
                else:
                    # Logs don't match at mid, try lower
                    right = mid - 1
                    print(f"[{self.node_id}]   Probe {mid}: CONFLICT, searching lower")
                    
            except grpc.RpcError as e:
                print(f"[{self.node_id}]   Probe {mid}: RPC error ({e.code()}), treating as conflict")
                right = mid - 1
            except Exception as e:
                print(f"[{self.node_id}]   Probe {mid}: Exception ({e}), treating as conflict")
                right = mid - 1
        
        # Return the next index after last matching
        result = last_matching + 1
        print(f"[{self.node_id}] Binary search result: start replication at index {result}")
        return result

    def _run_election_timer(self):
        """Background thread that triggers elections on timeout"""
        while self.running:
            time.sleep(0.1)  # Check every 100ms
            
            with self.state_lock:
         
                if self.state == NodeState.LEADER:
                    continue
                
                current_time = time.time()
                time_since_heartbeat = current_time - self.last_heartbeat
                
                # Random election timeout to prevent split votes
                if time_since_heartbeat > self.election_timeout:
                    # Double-check we haven't received a heartbeat just now
                    if current_time - self.last_heartbeat > self.election_timeout:
                        print(f"[{self.node_id}] Election timeout! Starting election...")
                        threading.Thread(target=self._start_election, daemon=True).start()
                        
                        # Reset last_heartbeat to prevent immediate re-trigger
                        self.last_heartbeat = current_time