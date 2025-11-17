"""
Flask Web Server for Distributed Inventory System
Provides REST API that wraps gRPC calls
"""

from flask import Flask, request, jsonify, send_from_directory, send_file
from flask_cors import CORS
import grpc
import os
import uuid
import time
from datetime import datetime
import raft_pb2
import raft_pb2_grpc
from server_app_leader.auth import authenticate_user, generate_token, verify_token, register_user

app = Flask(__name__, static_folder='web/static', template_folder='web/templates')
CORS(app)

# Get absolute path for static files
STATIC_DIR = os.path.join(os.path.dirname(__file__), 'web', 'static')

# Configuration
RAFT_SERVERS = os.getenv("RAFT_SERVERS", "127.0.0.1:50051,127.0.0.1:50052,127.0.0.1:50053").split(",")
current_leader = None

def find_leader():
    """Find the current Raft leader"""
    global current_leader
    for server_addr in RAFT_SERVERS:
        try:
            channel = grpc.insecure_channel(server_addr)
            stub = raft_pb2_grpc.RaftServiceStub(channel)
            response = stub.GetLeaderInfo(raft_pb2.GetLeaderRequest(), timeout=1.0)
            channel.close()
            
            if response.is_leader:
                current_leader = server_addr
                return server_addr
            elif response.leader_address:
                current_leader = response.leader_address
                return response.leader_address
        except:
            continue
    
    # Fallback to first server
    if not current_leader:
        current_leader = RAFT_SERVERS[0]
    return current_leader

def get_stub():
    """Get gRPC stub for current leader"""
    leader = find_leader()
    channel = grpc.insecure_channel(leader)
    return raft_pb2_grpc.RaftServiceStub(channel), channel

# Auth endpoints
@app.route('/api/auth/login', methods=['POST'])
def login():
    data = request.json
    username = data.get('username', '').strip()
    password = data.get('password', '').strip()
    user_type = data.get('user_type', 'customer')  # 'customer' or 'admin'
    
    if not username or not password:
        return jsonify({'success': False, 'message': 'Username and password required'}), 400
    
    # For now, check if user exists in auth system
    # Admin users are: admin, user, jenil
    is_admin = username in ['admin', 'user', 'jenil']
    
    if user_type == 'admin' and not is_admin:
        return jsonify({'success': False, 'message': 'Invalid admin credentials'}), 401
    
    if authenticate_user(username, password):
        token = generate_token(username)
        return jsonify({
            'success': True,
            'token': token,
            'username': username,
            'user_type': 'admin' if is_admin else 'customer'
        })
    
    return jsonify({'success': False, 'message': 'Invalid credentials'}), 401

@app.route('/api/auth/register', methods=['POST'])
def register():
    data = request.json
    username = data.get('username', '').strip()
    password = data.get('password', '').strip()
    user_type = data.get('user_type', 'customer')
    
    if not username or not password:
        return jsonify({'success': False, 'message': 'Username and password required'}), 400
    
    if register_user(username, password):
        token = generate_token(username)
        return jsonify({
            'success': True,
            'token': token,
            'username': username,
            'user_type': user_type
        })
    
    return jsonify({'success': False, 'message': 'Username already exists'}), 400

@app.route('/api/auth/verify', methods=['POST'])
def verify():
    data = request.json
    token = data.get('token', '')
    
    username = verify_token(token)
    if username:
        is_admin = username in ['admin', 'user', 'jenil']
        return jsonify({
            'success': True,
            'username': username,
            'user_type': 'admin' if is_admin else 'customer'
        })
    
    return jsonify({'success': False, 'message': 'Invalid token'}), 401

# Inventory endpoints
@app.route('/api/inventory', methods=['GET'])
def get_inventory():
    """Get all inventory items"""
    try:
        # Try to find leader if we don't have one
        global current_leader
        if not current_leader:
            find_leader()
        
        if not current_leader:
            return jsonify({'success': False, 'message': 'No leader found. Please ensure Raft servers are running.', 'items': []}), 500
        
        channel = grpc.insecure_channel(current_leader)
        stub = raft_pb2_grpc.RaftServiceStub(channel)
        response = stub.GetInventory(raft_pb2.GetInventoryRequest(), timeout=5.0)
        channel.close()
        
        items = [{'product': item.product, 'quantity': item.quantity} for item in response.items]
        return jsonify({'success': True, 'items': items})
    except grpc.RpcError as e:
        current_leader = None  # Force re-discovery
        return jsonify({'success': False, 'message': f'RPC Error: {e.code()}', 'items': []}), 500
    except Exception as e:
        return jsonify({'success': False, 'message': str(e), 'items': []}), 500

@app.route('/api/inventory/add', methods=['POST'])
def add_inventory():
    """Add inventory (admin only)"""
    data = request.json
    token = data.get('token', '')
    product = data.get('product', '').strip()
    quantity = data.get('quantity', 0)
    
    username = verify_token(token)
    if not username:
        return jsonify({'success': False, 'message': 'Unauthorized'}), 401
    
    if not product or quantity <= 0:
        return jsonify({'success': False, 'message': 'Invalid product or quantity'}), 400
    
    # Retry logic to find leader
    global current_leader
    max_retries = 3
    for attempt in range(max_retries):
        try:
            # Find leader if we don't have one or on retry
            if not current_leader or attempt > 0:
                find_leader()
            
            if not current_leader:
                return jsonify({'success': False, 'message': 'No leader found. Please ensure Raft servers are running.'}), 500
            
            channel = grpc.insecure_channel(current_leader)
            stub = raft_pb2_grpc.RaftServiceStub(channel)
            
            response = stub.AddInventory(
                raft_pb2.AddInventoryRequest(
                    username=username,
                    product=product,
                    quantity=quantity,
                    request_id=str(uuid.uuid4()),
                    timestamp=datetime.now().isoformat()
                ),
                timeout=10.0
            )
            channel.close()
            
            if response.success:
                return jsonify({'success': True, 'message': response.message})
            else:
                # If not leader, update leader hint and retry
                if "not leader" in response.message.lower() and response.leader_hint:
                    current_leader = response.leader_hint
                    continue
                return jsonify({'success': False, 'message': response.message}), 500
                
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.UNAVAILABLE:
                current_leader = None  # Force re-discovery
                if attempt < max_retries - 1:
                    time.sleep(0.5)
                    continue
            return jsonify({'success': False, 'message': f'RPC Error: {e.code()}'}), 500
        except Exception as e:
            return jsonify({'success': False, 'message': str(e)}), 500
    
    return jsonify({'success': False, 'message': 'Failed to add inventory after retries'}), 500

@app.route('/api/inventory/update', methods=['POST'])
def update_inventory():
    """Update inventory to absolute quantity (admin only)"""
    data = request.json
    token = data.get('token', '')
    product = data.get('product', '').strip()
    quantity = data.get('quantity', 0)
    
    username = verify_token(token)
    if not username:
        return jsonify({'success': False, 'message': 'Unauthorized'}), 401
    
    if not product or quantity < 0:
        return jsonify({'success': False, 'message': 'Invalid product or quantity'}), 400
    
    # Retry logic to find leader
    global current_leader
    max_retries = 3
    for attempt in range(max_retries):
        try:
            # Find leader if we don't have one or on retry
            if not current_leader or attempt > 0:
                find_leader()
            
            if not current_leader:
                return jsonify({'success': False, 'message': 'No leader found. Please ensure Raft servers are running.'}), 500
            
            channel = grpc.insecure_channel(current_leader)
            stub = raft_pb2_grpc.RaftServiceStub(channel)
            
            response = stub.UpdateInventory(
                raft_pb2.UpdateInventoryRequest(
                    username=username,
                    product=product,
                    quantity=quantity,
                    request_id=str(uuid.uuid4()),
                    timestamp=datetime.now().isoformat()
                ),
                timeout=10.0
            )
            channel.close()
            
            if response.success:
                return jsonify({'success': True, 'message': response.message})
            else:
                # If not leader, update leader hint and retry
                if "not leader" in response.message.lower() and response.leader_hint:
                    current_leader = response.leader_hint
                    continue
                return jsonify({'success': False, 'message': response.message}), 500
                
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.UNAVAILABLE:
                current_leader = None  # Force re-discovery
                if attempt < max_retries - 1:
                    time.sleep(0.5)
                    continue
            return jsonify({'success': False, 'message': f'RPC Error: {e.code()}'}), 500
        except Exception as e:
            return jsonify({'success': False, 'message': str(e)}), 500
    
    return jsonify({'success': False, 'message': 'Failed to update inventory after retries'}), 500

@app.route('/api/inventory/purchase', methods=['POST'])
def purchase_inventory():
    """Purchase items (reduce inventory) - customer only"""
    data = request.json
    token = data.get('token', '')
    items = data.get('items', [])  # List of {product, quantity}
    
    username = verify_token(token)
    if not username:
        return jsonify({'success': False, 'message': 'Unauthorized'}), 401
    
    if not items:
        return jsonify({'success': False, 'message': 'No items to purchase'}), 400
    
    results = []
    try:
        stub, channel = get_stub()
        
        # First, get current inventory to check availability
        inv_response = stub.GetInventory(raft_pb2.GetInventoryRequest(), timeout=5.0)
        inventory = {item.product: item.quantity for item in inv_response.items}
        
        # Check availability
        for item in items:
            product = item.get('product', '').strip()
            quantity = item.get('quantity', 0)
            
            if product not in inventory:
                results.append({'product': product, 'success': False, 'message': 'Product not found'})
            elif inventory[product] < quantity:
                results.append({'product': product, 'success': False, 'message': f'Insufficient stock. Available: {inventory[product]}'})
            else:
                # Reduce inventory
                new_quantity = inventory[product] - quantity
                response = stub.UpdateInventory(
                    raft_pb2.UpdateInventoryRequest(
                        username=username,
                        product=product,
                        quantity=new_quantity,
                        request_id=str(uuid.uuid4()),
                        timestamp=datetime.now().isoformat()
                    ),
                    timeout=10.0
                )
                
                if response.success:
                    results.append({'product': product, 'success': True, 'message': f'Purchased {quantity} units'})
                else:
                    results.append({'product': product, 'success': False, 'message': response.message})
        
        channel.close()
        
        all_success = all(r.get('success', False) for r in results)
        return jsonify({
            'success': all_success,
            'results': results
        })
    except Exception as e:
        return jsonify({'success': False, 'message': str(e), 'results': results}), 500

# AI Query endpoint
@app.route('/api/ai/query', methods=['POST'])
def query_ai():
    """Query AI about inventory"""
    data = request.json
    token = data.get('token', '')
    query = data.get('query', '').strip()
    
    username = verify_token(token)
    if not username:
        return jsonify({'success': False, 'message': 'Unauthorized'}), 401
    
    if not query:
        return jsonify({'success': False, 'message': 'Query required'}), 400
    
    # Retry logic to find leader
    global current_leader
    max_retries = 3
    for attempt in range(max_retries):
        try:
            # Find leader if we don't have one or on retry
            if not current_leader or attempt > 0:
                find_leader()
            
            if not current_leader:
                return jsonify({'success': False, 'message': 'No leader found. Please ensure Raft servers are running.'}), 500
            
            channel = grpc.insecure_channel(current_leader)
            stub = raft_pb2_grpc.RaftServiceStub(channel)
            
            response = stub.QueryLLM(
                raft_pb2.QueryLLMRequest(
                    query=query,
                    username=username,
                    request_id=str(uuid.uuid4())
                ),
                timeout=60.0
            )
            channel.close()
            
            if response.success:
                return jsonify({'success': True, 'response': response.response})
            else:
                # If not leader, update leader hint and retry
                if "not leader" in response.error.lower():
                    if attempt < max_retries - 1:
                        current_leader = None  # Force re-discovery
                        time.sleep(0.5)
                        continue
                return jsonify({'success': False, 'message': response.error}), 500
                
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.UNAVAILABLE:
                current_leader = None  # Force re-discovery
                if attempt < max_retries - 1:
                    time.sleep(0.5)
                    continue
            return jsonify({'success': False, 'message': f'RPC Error: {e.code()}'}), 500
        except Exception as e:
            return jsonify({'success': False, 'message': str(e)}), 500
    
    return jsonify({'success': False, 'message': 'Failed to query AI after retries'}), 500

# Serve static files
@app.route('/')
def index():
    return send_file(os.path.join(STATIC_DIR, 'index.html'))

@app.route('/<path:filename>')
def serve_static(filename):
    """Serve static files from web/static directory"""
    file_path = os.path.join(STATIC_DIR, filename)
    if os.path.exists(file_path) and os.path.isfile(file_path):
        return send_file(file_path)
    # If file not found, try index.html for SPA routing
    return send_file(os.path.join(STATIC_DIR, 'index.html'))

if __name__ == '__main__':
    print("Starting Flask web server on http://localhost:5000")
    app.run(host='0.0.0.0', port=5000, debug=True)

