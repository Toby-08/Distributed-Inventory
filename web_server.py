"""
Flask Web Server for Distributed Inventory System
Provides REST API that wraps gRPC calls to the Raft cluster
"""

import os
import logging
import grpc
import time

from flask import Flask, send_from_directory, jsonify, request
from flask_cors import CORS

import app_pb2
import app_pb2_grpc
import raft_pb2
import raft_pb2_grpc

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__, static_folder="web/static", static_url_path="")

CORS(app, resources={r"/api/*": {"origins": "*"}}, supports_credentials=True)

RAFT_NODES = os.getenv(
    "RAFT_NODES", "raft-node1:50051,raft-node2:50052,raft-node3:50053"
).split(",")

logger.info(f"Available Raft nodes: {RAFT_NODES}")

# -------------------------------
# Leader Discovery
# -------------------------------

leader_cache = {
    "address": None,
    "last_check": 0,
    "cache_duration": 10,  # seconds
}


def get_current_leader():
    """Ask every Raft node who the leader is."""
    logger.info("Discovering leader...")
    now = time.time()

    # Return cached leader if still fresh
    if leader_cache["address"] and (now - leader_cache["last_check"]) < leader_cache["cache_duration"]:
        logger.info(f"Using cached leader: {leader_cache['address']}")
        return leader_cache["address"]

    logger.info("Discovering leader...")

    for node_addr in RAFT_NODES:
        try:
            channel = grpc.insecure_channel(node_addr)
            stub = raft_pb2_grpc.RaftServiceStub(channel)

            # Query if this node is the leader
            response = stub.GetLeaderInfo(raft_pb2.GetLeaderRequest(), timeout=2.0)

            channel.close()

            if response.is_leader:
                logger.info(f"Found leader at {node_addr} (term={response.current_term})")
                leader_cache["address"] = node_addr
                leader_cache["last_check"] = now
                return node_addr
            else:
                logger.info(f"{node_addr} is follower (term={response.current_term})")

        except grpc.RpcError as e:
            logger.warning(f"Failed to reach {node_addr}: {e.code()}")
        except Exception as e:
            logger.warning(f"Error probing {node_addr}: {e}")

    # No leader found
    logger.error(f"No leader found among {RAFT_NODES}")
    leader_cache["address"] = None
    leader_cache["last_check"] = now
    return None


# -------------------------------
# Generic leader execution
# -------------------------------


def call_leader(fn, max_retries=3):
    """
    Execute a RPC function 'fn(stub)' on the current leader.
    Automatically retries if leader changed.
    """
    for attempt in range(max_retries):
        leader = get_current_leader()
        
        if not leader:
            raise Exception("No leader available")

        try:
            channel = grpc.insecure_channel(leader)
            stub = app_pb2_grpc.AppServiceStub(channel)
            resp = fn(stub)
            channel.close()
            return resp

        except grpc.RpcError as e:
            channel.close()
            
            # If not leader or unavailable, invalidate cache and retry
            if e.code() in [grpc.StatusCode.FAILED_PRECONDITION, grpc.StatusCode.UNAVAILABLE]:
                logger.warning(f"Leader changed or unavailable. Retrying... (attempt {attempt + 1})")
                leader_cache["address"] = None
                if attempt < max_retries - 1:
                    time.sleep(0.5)
                    continue
            raise


# -------------------------------
# Static Files
# -------------------------------


@app.route("/")
def index():
    """Serve main page"""
    return send_from_directory("web/static", "index.html")


@app.route("/<path:path>")
def static_files(path):
    """Serve static files (CSS, JS, HTML)"""
    try:
        return send_from_directory("web/static", path)
    except Exception as e:
        logger.warning(f"Static file not found: {path}")
        return send_from_directory("web/static", "index.html")


# -------------------------------
# API ENDPOINTS
# -------------------------------


@app.route("/api/auth/login", methods=["POST"])
def api_login():
    """User login"""
    data = request.get_json(force=True) or {}
    username = data.get("username", "")
    password = data.get("password", "")

    if not username or not password:
        return jsonify({"status": "error", "message": "Missing credentials"}), 400

    def rpc(stub):
        return stub.login(
            app_pb2.LoginRequest(username=username, password=password), timeout=5
        )

    try:
        resp = call_leader(rpc)
        return jsonify({
            "status": resp.status,
            "token": resp.token,
            "message": resp.message
        })
    except grpc.RpcError as e:
        logger.error(f"Login RPC error: {e.code()} - {e.details()}")
        return jsonify({"status": "error", "message": f"Login failed: {e.code()}"}), 503
    except Exception as e:
        logger.error(f"Login error: {e}")
        return jsonify({"status": "error", "message": "Login failed"}), 503


@app.route("/api/auth/signup", methods=["POST"])
def api_signup():
    """User signup"""
    data = request.get_json(force=True) or {}
    username = data.get("username", "")
    password = data.get("password", "")

    if not username or not password:
        return jsonify({"status": "error", "message": "Missing credentials"}), 400

    def rpc(stub):
        return stub.signup(
            app_pb2.SignupRequest(username=username, password=password), 
            timeout=5
        )

    try:
        resp = call_leader(rpc)
        return jsonify({
            "status": resp.status,
            "message": resp.message,
            "token": resp.token
        })
    except grpc.RpcError as e:
        logger.error(f"Signup RPC error: {e.code()} - {e.details()}")
        return jsonify({"status": "error", "message": f"Signup failed: {e.code()}"}), 503
    except Exception as e:
        logger.error(f"Signup error: {e}")
        return jsonify({"status": "error", "message": "Signup failed"}), 503


@app.route("/api/inventory", methods=["GET"])
def api_inventory_get():
    """Get inventory"""
    token = request.headers.get("Authorization", "").replace("Bearer ", "")

    if not token:
        return jsonify({"status": "error", "message": "No token provided"}), 401

    def rpc(stub):
        return stub.get(app_pb2.GetRequest(token=token), timeout=5)

    try:
        resp = call_leader(rpc)
        
        # Convert inventory to list of dicts
        inventory_list = [
            {"product": item.product, "quantity": item.quantity}
            for item in resp.inventory
        ]
        
        return jsonify({
            "status": resp.status,
            "inventory": inventory_list
        })
    except grpc.RpcError as e:
        logger.error(f"Get inventory RPC error: {e.code()} - {e.details()}")
        if e.code() == grpc.StatusCode.UNAUTHENTICATED:
            return jsonify({"status": "error", "message": "Invalid token"}), 401
        return jsonify({"status": "error", "message": f"Failed to load inventory: {e.code()}"}), 503
    except Exception as e:
        logger.error(f"Get inventory error: {e}")
        return jsonify({"status": "error", "message": "Failed to load inventory"}), 503


@app.route("/api/inventory", methods=["POST"])
def api_inventory_post():
    """Update inventory"""
    token = request.headers.get("Authorization", "").replace("Bearer ", "")
    data = request.get_json(force=True) or {}

    if not token:
        return jsonify({"status": "error", "message": "No token provided"}), 401

    operation = data.get("operation")
    product = data.get("product")
    quantity = data.get("quantity")

    if not operation or not product or quantity is None:
        return jsonify({"status": "error", "message": "Missing required fields"}), 400

    def rpc(stub):
        return stub.post(
            app_pb2.PostRequest(
                token=token, 
                operation=operation, 
                product=product, 
                quantity=int(quantity)
            ),
            timeout=5,
        )

    try:
        resp = call_leader(rpc)
        return jsonify({
            "status": resp.status, 
            "message": resp.message
        })
    except grpc.RpcError as e:
        logger.error(f"Post inventory RPC error: {e.code()} - {e.details()}")
        if e.code() == grpc.StatusCode.UNAUTHENTICATED:
            return jsonify({"status": "error", "message": "Invalid token"}), 401
        elif e.code() == grpc.StatusCode.PERMISSION_DENIED:
            return jsonify({"status": "error", "message": "Permission denied"}), 403
        return jsonify({"status": "error", "message": f"Update failed: {e.code()}"}), 503
    except Exception as e:
        logger.error(f"Post inventory error: {e}")
        return jsonify({"status": "error", "message": "Update failed"}), 503


@app.route("/api/query", methods=["POST"])
def api_query():
    """AI query"""
    token = request.headers.get("Authorization", "").replace("Bearer ", "")
    data = request.get_json(force=True) or {}
    query = data.get("query", "")

    if not token:
        return jsonify({"status": "error", "message": "No token provided"}), 401

    if not query:
        return jsonify({"status": "error", "message": "No query provided"}), 400

    def rpc(stub):
        return stub.query(
            app_pb2.QueryRequest(token=token, query=query), 
            timeout=90          )

    try:
        resp = call_leader(rpc)
        return jsonify({
            "status": resp.status, 
            "answer": resp.answer
        })
    except grpc.RpcError as e:
        logger.error(f"Query RPC error: {e.code()} - {e.details()}")
        if e.code() == grpc.StatusCode.UNAUTHENTICATED:
            return jsonify({"status": "error", "message": "Invalid token"}), 401
        elif e.code() == grpc.StatusCode.DEADLINE_EXCEEDED:
            return jsonify({"status": "error", "message": "AI query timeout (try simpler question)"}), 504
        return jsonify({"status": "error", "message": f"Query failed: {e.code()}"}), 503
    except Exception as e:
        logger.error(f"Query error: {e}")
        return jsonify({"status": "error", "message": "Query failed"}), 503


@app.route("/health")
def health():
    """Health check"""
    leader = get_current_leader()
    return jsonify({
        "status": "healthy",
        "leader": leader,
        "nodes": RAFT_NODES
    })


if __name__ == "__main__":
    logger.info("Starting web server on port 5000")
    logger.info(f"Raft cluster: {RAFT_NODES}")
    app.run(host="0.0.0.0", port=5000, debug=False)
