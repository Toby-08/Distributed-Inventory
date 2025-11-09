import grpc
import time
import raft_pb2
import raft_pb2_grpc
from server_app_leader.raft_node import NodeState

class RaftServicer(raft_pb2_grpc.RaftServiceServicer):
    def __init__(self, raft_node):
        self.node = raft_node
    
    def RequestVote(self, request, context):
        """Handle RequestVote RPC"""
        node = self.node
        
        with node.state_lock:
            print(f"[{node.node_id}] Received vote request from {request.candidateId} for term {request.term}")
            
            # Reject if term < currentTerm
            if request.term < node.current_term:
                print(f"[{node.node_id}] Rejected vote for {request.candidateId} (old term {request.term} < {node.current_term})")
                return raft_pb2.VoteResponse(term=node.current_term, voteGranted=False)
            
            # Update term if higher
            if request.term > node.current_term:
                print(f"[{node.node_id}] Reverted to FOLLOWER for term {request.term}")
                node.current_term = request.term
                node.state = NodeState.FOLLOWER
                node.voted_for = None
                node._persist_state()
            
            # Grant vote if conditions met
            vote_granted = False
            
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
                    print(f"[{node.node_id}] Voted for {request.candidateId} in term {request.term}")
                else:
                    print(f"[{node.node_id}] Rejected vote (log not up-to-date)")
            else:
                print(f"[{node.node_id}] Rejected vote (already voted for {node.voted_for})")
            
            return raft_pb2.VoteResponse(term=node.current_term, voteGranted=vote_granted)
    
    def AppendEntries(self, request, context):
        """Handle AppendEntries RPC (heartbeats and log replication)"""
        node = self.node
        
        with node.state_lock:
            # Reject old terms
            if request.term < node.current_term:
                return raft_pb2.AppendEntriesResponse(term=node.current_term, success=False)
            
            # Update term if higher
            if request.term > node.current_term:
                print(f"[{node.node_id}] Reverted to FOLLOWER for term {request.term}")
                node.current_term = request.term
                node.voted_for = None
                node._persist_state()
            
            # Always step down to follower when receiving valid AppendEntries
            if node.state != NodeState.FOLLOWER:
                print(f"[{node.node_id}] Reverted to FOLLOWER for term {request.term}")
            node.state = NodeState.FOLLOWER
            
            # ⭐ Reset election timer!
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
                    print(f"[{node.node_id}] 💚 Heartbeat from {request.leaderId} (term {request.term}, count={node._heartbeat_count})")
                
                return raft_pb2.AppendEntriesResponse(term=node.current_term, success=True)
            
            # Log replication (for actual entries) - always log these
            print(f"[{node.node_id}] 📨 Received {len(request.entries)} entries from {request.leaderId}")
            
            # Check log consistency
            if request.prevLogIndex > 0:
                if request.prevLogIndex > len(node.log):
                    # Missing entries
                    print(f"[{node.node_id}] ❌ Missing entries (prevLogIndex={request.prevLogIndex}, log length={len(node.log)})")
                    return raft_pb2.AppendEntriesResponse(term=node.current_term, success=False)
                
                if node.log[request.prevLogIndex - 1]['term'] != request.prevLogTerm:
                    # Log conflict - delete conflicting entries
                    print(f"[{node.node_id}] ⚠️ Log conflict at index {request.prevLogIndex}")
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
                    print(f"[{node.node_id}] ✅ Applied log {entry['index']}: {entry['operation']} {entry['product']}")
            
            # Update commit index
            if request.leaderCommit > node.commit_index:
                node.commit_index = min(request.leaderCommit, len(node.log))
            
            return raft_pb2.AppendEntriesResponse(term=node.current_term, success=True)