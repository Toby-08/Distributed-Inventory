import os, grpc
from concurrent import futures
import app_pb2_grpc
import raft_pb2_grpc  # <-- add
from server_app_leader.raft_node import RaftNode
from server_app_leader.app_servicer import AppServicer
from server_app_leader.raft_servicer import RaftServicer  # <-- add


def serve():
    # Get config from environment
    node_id = os.getenv("NODE_ID", "node1")
    port = int(os.getenv("NODE_PORT", "50051"))
    peers = [p.strip() for p in os.getenv("PEERS", "").split(",") if p.strip()]
    llm = os.getenv("LLM_SERVER", "llm-server:50054")
    
    print(f"[RAFT] Starting node: {node_id}")
    print(f"[RAFT] Port: {port}")
    print(f"[RAFT] Peers: {peers}")
    print(f"[RAFT] LLM Server: {llm}")
    
    # Create Raft node
    raft_node = RaftNode(
        node_id=node_id,
        peers=peers,
        port=port,
        llm_server_address=llm
    )
    
    # Create gRPC server
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=16))
    
    # Register BOTH services
    app_pb2_grpc.add_AppServiceServicer_to_server(
        AppServicer(raft_node), server
    )
    raft_pb2_grpc.add_RaftServiceServicer_to_server(RaftServicer(raft_node), server)  # <-- add

    # Bind with IPv6 then IPv4 fallback
   
    server.add_insecure_port(f"0.0.0.0:{port}")
    

    server.start()
    print(f"[{node_id}] gRPC server started on {port}")
    
    # Start Raft consensus
    raft_node.start()
    
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        print("[RAFT] Shutting down...")
        raft_node.stop()
        server.stop(0)


if __name__ == "__main__":
    serve()