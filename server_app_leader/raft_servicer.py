import grpc
import time
import raft_pb2
import raft_pb2_grpc
import llm_pb2
import llm_pb2_grpc
from server_app_leader.raft_node import NodeState

class RaftServicer(raft_pb2_grpc.RaftServiceServicer):
    def __init__(self, raft_node):
        self.raft_node = raft_node
    
    def RequestVote(self, request, context):
        """Handle RequestVote RPC"""
        node = self.raft_node
        
        with node.state_lock:
            print(f"[{node.node_id}] Received vote request from {request.candidateId} for term {request.term} (our term: {node.current_term}, voted_for: {node.voted_for})")
            
            #Reject if term < currentTerm
            if request.term < node.current_term:
                print(f"[{node.node_id}] Rejected vote for {request.candidateId} (old term {request.term} < {node.current_term})")
                return raft_pb2.VoteResponse(term=node.current_term, voteGranted=False)
            
            # Update term if higher AND RESET voted_for
            if request.term > node.current_term:
                print(f"[{node.node_id}] Updating term from {node.current_term} to {request.term} and RESETTING voted_for")
                node.current_term = request.term
                node.state = NodeState.FOLLOWER
                node.voted_for = None  # MUST RESET - allows voting in new term
                node._persist_state()
        
        # Grant vote if conditions met
        vote_granted = False
        
        # Can vote if: (1) haven't voted OR (2) already voted for this candidate
        if (node.voted_for is None or node.voted_for == request.candidateId):
            # Check log up-to-date
            last_log_index = len(node.log)
            last_log_term = node.log[-1]['term'] if node.log else 0
            
            log_ok = (request.lastLogTerm > last_log_term or 
                     (request.lastLogTerm == last_log_term and request.lastLogIndex >= last_log_index))
            
            if log_ok:
                vote_granted = True
                node.voted_for = request.candidateId
                node.last_heartbeat = time.time()  # Reset election timer
                node._persist_state()
                print(f"[{node.node_id}] GRANTED vote to {request.candidateId} in term {request.term}")
            else:
                print(f"[{node.node_id}] REJECTED vote (log not up-to-date: candidate=[term={request.lastLogTerm}, idx={request.lastLogIndex}], ours=[term={last_log_term}, idx={last_log_index}])")
        else:
            print(f"[{node.node_id}] REJECTED vote (already voted for {node.voted_for} in term {node.current_term})")
        
        return raft_pb2.VoteResponse(term=node.current_term, voteGranted=vote_granted)
    
    def AppendEntries(self, request, context):
        """Handle AppendEntries RPC (heartbeats and log replication)"""
        node = self.raft_node
        
        with node.state_lock:
            # Reject old terms and tell sender about current term
            if request.term < node.current_term:
                # Old leader trying to send heartbeats - tell them the current term
                print(f"[{node.node_id}] Rejected AppendEntries from {request.leaderId} (old term {request.term} < {node.current_term})")
                return raft_pb2.AppendEntriesResponse(term=node.current_term, success=False)
            
            # Update term if higher
            if request.term > node.current_term:
                print(f"[{node.node_id}] Updating term from {node.current_term} to {request.term}")
                node.current_term = request.term
                node.voted_for = None
                node._persist_state()
            
            # Always step down to follower when receiving valid AppendEntries
            if node.state != NodeState.FOLLOWER:
                print(f"[{node.node_id}] Stepping down to FOLLOWER (received AppendEntries from {request.leaderId})")
            node.state = NodeState.FOLLOWER
            
            # Reset election timer - we have a valid leader
            node.last_heartbeat = time.time()
            
            # Handle heartbeats (empty entries)
            if not request.entries:
                # Just a heartbeat - acknowledge silently (or log periodically)
                # Only log every Nth heartbeat to avoid spam
                if not hasattr(node, '_heartbeat_count'):
                    node._heartbeat_count = 0
                node._heartbeat_count += 1
                
                # Log every 50th heartbeat (~5 seconds at 100ms interval)
                if node._heartbeat_count % 50 == 0:
                    print(f"[{node.node_id}] Heartbeat from {request.leaderId} (term {request.term}, count={node._heartbeat_count})")
                
                return raft_pb2.AppendEntriesResponse(term=node.current_term, success=True)
            
            # Log replication (for actual entries) - always log these
            print(f"[{node.node_id}] Received {len(request.entries)} entries from {request.leaderId}")
            
            # Check log consistency
            if request.prevLogIndex > 0:
                if request.prevLogIndex > len(node.log):
                    # Missing entries
                    print(f"[{node.node_id}] Missing entries (prevLogIndex={request.prevLogIndex}, log length={len(node.log)})")
                    return raft_pb2.AppendEntriesResponse(term=node.current_term, success=False)
                
                if node.log[request.prevLogIndex - 1]['term'] != request.prevLogTerm:
                    # Log conflict - delete conflicting entries
                    print(f"[{node.node_id}] Log conflict at index {request.prevLogIndex}")
                    node.log = node.log[:request.prevLogIndex - 1]
                    return raft_pb2.AppendEntriesResponse(term=node.current_term, success=False)
            
            # Append new entries
            for i, entry_proto in enumerate(request.entries):
                entry_index = request.prevLogIndex + i + 1
                
                entry = {
                    'term': entry_proto.term,
                    'index': entry_proto.index,
                    'operation': entry_proto.operation,
                    'product': entry_proto.product,
                    'qty_change': entry_proto.qty_change,
                    'new_qty': entry_proto.new_qty,
                    'username': entry_proto.username,
                    'timestamp': entry_proto.timestamp,
                    'request_id': entry_proto.request_id
                }
                
                # If entry exists with different term, delete it and all following
                if entry_index <= len(node.log):
                    if node.log[entry_index - 1]['term'] != entry['term']:
                        node.log = node.log[:entry_index - 1]
                
                # Append if new
                if entry_index > len(node.log):
                    node.log.append(entry)
                    node._persist_log_entry(entry)
                    node._apply_loaded(entry)
                    print(f"[{node.node_id}] Applied log {entry['index']}: {entry['operation']} {entry['product']}")
            
            # Update commit index
            if request.leaderCommit > node.commit_index:
                node.commit_index = min(request.leaderCommit, len(node.log))
            
            return raft_pb2.AppendEntriesResponse(term=node.current_term, success=True)
    
    def AddInventory(self, request, context):
        """Handle client add inventory request"""
        # Check if this node is the leader
        if self.raft_node.state != NodeState.LEADER:
            leader_addr = self.raft_node.get_leader_address()
            return raft_pb2.AddInventoryResponse(
                success=False,
                message="Not the leader",
                leader_hint=leader_addr or ""
            )
        
        try:
            # Check for duplicate (idempotency)
            for existing in self.raft_node.log:
                if existing.get('request_id') == request.request_id:
                    print(f"[{self.raft_node.node_id}] Duplicate request {request.request_id}, returning success")
                    return raft_pb2.AddInventoryResponse(
                        success=True,
                        message=f"Added {request.quantity} {request.product} (idempotent)"
                    )
            
            # Create log entry
            entry = {
                'term': self.raft_node.current_term,
                'index': len(self.raft_node.log) + 1,
                'operation': 'add_inventory',
                'product': request.product,
                'qty_change': request.quantity,
                'new_qty': request.quantity,
                'username': request.username,
                'timestamp': request.timestamp,
                'request_id': request.request_id
            }
            
            # Append to log
            self.raft_node.log.append(entry)
            self.raft_node._persist_log_entry(entry)
            
            # Apply to state machine
            self.raft_node._apply_loaded(entry)
            
            print(f"[{self.raft_node.node_id}] Appended log {entry['index']}: {request.product} +{request.quantity}")
            
            # TRIGGER IMMEDIATE REPLICATION
            import threading
            threading.Thread(target=self.raft_node._send_heartbeats, daemon=True).start()
            
            # SEND TO LLM (if available)
            if self.raft_node.llm_available:
                threading.Thread(target=self.raft_node._send_log_to_llm, args=(entry,), daemon=True).start()
            
            return raft_pb2.AddInventoryResponse(
                success=True,
                message=f"Added {request.quantity} {request.product}"
            )
            
        except Exception as e:
            print(f"[{self.raft_node.node_id}] AddInventory error: {e}")
            import traceback
            traceback.print_exc()
            return raft_pb2.AddInventoryResponse(
                success=False,
                message=f"Internal error: {str(e)}"
            )
    
    def UpdateInventory(self, request, context):
        """Handle client update inventory request"""
        # Check if this node is the leader
        if self.raft_node.state != NodeState.LEADER:
            leader_addr = self.raft_node.get_leader_address()
            return raft_pb2.UpdateInventoryResponse(
                success=False,
                message="Not the leader",
                leader_hint=leader_addr or ""
            )
        
        try:
            # Check for duplicate (idempotency)
            for existing in self.raft_node.log:
                if existing.get('request_id') == request.request_id:
                    print(f"[{self.raft_node.node_id}] Duplicate request {request.request_id}, returning success")
                    return raft_pb2.UpdateInventoryResponse(
                        success=True,
                        message=f"Updated {request.product} to {request.quantity} (idempotent)"
                    )
            
            # Create log entry
            entry = {
                'term': self.raft_node.current_term,
                'index': len(self.raft_node.log) + 1,
                'operation': 'update_inventory',
                'product': request.product,
                'qty_change': 0,  # Not used for updates
                'new_qty': request.quantity,
                'username': request.username,
                'timestamp': request.timestamp,
                'request_id': request.request_id
            }
            
            # Append to log
            self.raft_node.log.append(entry)
            self.raft_node._persist_log_entry(entry)
            
            # Apply to state machine
            self.raft_node._apply_loaded(entry)
            
            print(f"[{self.raft_node.node_id}] Appended log {entry['index']}: {request.product} = {request.quantity}")
            
            # TRIGGER IMMEDIATE REPLICATION (same as AddInventory)
            import threading
            threading.Thread(target=self.raft_node._send_heartbeats, daemon=True).start()
            
            # SEND TO LLM (if available)
            if self.raft_node.llm_available:
                threading.Thread(target=self.raft_node._send_log_to_llm, args=(entry,), daemon=True).start()
            
            return raft_pb2.UpdateInventoryResponse(
                success=True,
                message=f"Updated {request.product} to {request.quantity}"
            )
        
        except Exception as e:
            print(f"[{self.raft_node.node_id}] UpdateInventory error: {e}")
            import traceback
            traceback.print_exc()
            return raft_pb2.UpdateInventoryResponse(
                success=False,
                message=f"Internal error: {str(e)}"
            )
    
    def GetInventory(self, request, context):
        """Return current inventory state"""
        try:
            # Build inventory from log
            inventory = {}
            for entry in self.raft_node.log:
                product = entry.get('product')
                if not product:
                    continue
                
                if entry['operation'] == 'add_inventory':
                    inventory[product] = inventory.get(product, 0) + entry.get('qty_change', 0)
                elif entry['operation'] == 'update_inventory':
                    inventory[product] = entry.get('new_qty', 0)
            
            # Convert to protobuf
            items = [
                raft_pb2.InventoryItem(product=k, quantity=v)
                for k, v in inventory.items()
            ]
            
            return raft_pb2.GetInventoryResponse(items=items)
            
        except Exception as e:
            print(f"[{self.raft_node.node_id}] GetInventory error: {e}")
            return raft_pb2.GetInventoryResponse(items=[])
    
    def GetLeaderInfo(self, request, context):
        """Return leader information"""
        is_leader = (self.raft_node.state == NodeState.LEADER)
        leader_addr = self.raft_node.get_leader_address() if not is_leader else ""
        
        return raft_pb2.GetLeaderResponse(
            is_leader=is_leader,
            leader_address=leader_addr,
            current_term=self.raft_node.current_term
        )
    
    def QueryLLM(self, request, context):
        """Forward inventory-related AI query to the LLM server (leader-only)."""
        try:
            # 🧠 Ensure only leader processes LLM queries
            if not self.raft_node.is_leader:
                leader_hint = self.raft_node.leader_id or ""
                print(f"[RAFT] ⚠️ Node {self.raft_node.node_id} received QueryLLM but is not leader. Redirecting to {leader_hint}")
                return raft_pb2.QueryLLMResponse(
                    success=False,
                    response="",
                    error="Not leader. Please redirect.",
                )

            # 🎯 Connect to LLM gRPC server (e.g. localhost:50054)
            llm_addr = getattr(self.raft_node, "llm_server_address", "127.0.0.1:50054")
            print(f"[RAFT] Forwarding QueryLLM to LLM server at {llm_addr}")

            channel = grpc.insecure_channel(llm_addr)

            # 🔍 Test connection first (2s timeout)
            try:
                grpc.channel_ready_future(channel).result(timeout=2.0)
            except grpc.FutureTimeoutError:
                print("[RAFT] ❌ LLM server unavailable.")
                return raft_pb2.QueryLLMResponse(
                    success=False,
                    response="",
                    error="LLM server unavailable."
                )

            stub = llm_pb2_grpc.LLMServiceStub(channel)

            # 🚀 Forward the query to LLM (20s timeout to handle Gemini delay)
            llm_response = stub.QueryInventory(
                llm_pb2.QueryRequest(
                    query=request.query,
                    username=request.username,
                    request_id=request.request_id
                ),
                timeout=20.0
            )

            channel.close()

            print(f"[RAFT] ✅ Received response from LLM (len={len(llm_response.response)} chars)")

            # 🔁 Relay back to client
            return raft_pb2.QueryLLMResponse(
                success=llm_response.success,
                response=llm_response.response,
                error=llm_response.error
            )

        except grpc.RpcError as e:
            print(f"[RAFT] ❌ LLM gRPC error: {e.code()}")
            return raft_pb2.QueryLLMResponse(
                success=False,
                response="",
                error=f"LLM RPC error: {e.code()}"
            )
        except Exception as e:
            print(f"[RAFT] ❌ LLM forwarding error: {e}")
            return raft_pb2.QueryLLMResponse(
                success=False,
                response="",
                error=f"Error: {str(e)}"
            )