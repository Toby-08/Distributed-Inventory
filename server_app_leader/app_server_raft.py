import grpc
from concurrent import futures
import sys
import os

from server_app_leader.raft_node import RaftNode
from server_app_leader.raft_servicer import RaftServicer
import raft_pb2_grpc


def serve():
    # ✅ Get configuration from environment variables (Docker)
    node_id = os.getenv("NODE_ID", "leader")
    port = int(os.getenv("NODE_PORT", "50051"))
    peers_str = os.getenv("PEERS", "")
    llm_server = os.getenv("LLM_SERVER", "127.0.0.1:50054")
    
    # Parse peers list
    peers = [p.strip() for p in peers_str.split(",") if p.strip()]
    
    print(f"[{node_id}] ==========================================")
    print(f"[{node_id}] Starting Raft Node")
    print(f"[{node_id}] ==========================================")
    print(f"[{node_id}] Node ID: {node_id}")
    print(f"[{node_id}] Port: {port}")
    print(f"[{node_id}] Peers: {peers}")
    print(f"[{node_id}] LLM Server: {llm_server}")
    print(f"[{node_id}] ==========================================")
    
    # Create Raft node
    raft_node = RaftNode(
        node_id=node_id,
        peers=peers,
        port=port,
        llm_server_address=llm_server
    )
    
    # Start consensus engine
    raft_node.start()
    
    # Create gRPC server
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    raft_pb2_grpc.add_RaftServiceServicer_to_server(
        RaftServicer(raft_node), 
        server
    )
    
    # Bind to 0.0.0.0 for Docker (not 127.0.0.1)
    server.add_insecure_port(f"0.0.0.0:{port}")
    server.start()
    
    print(f"[{node_id}] Raft server started on 0.0.0.0:{port}")
    print(f"[{node_id}] Ready to serve requests")
    
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        print(f"\n[{node_id}] Shutting down...")
        raft_node.running = False
        server.stop(0)


if __name__ == "__main__":
    serve()