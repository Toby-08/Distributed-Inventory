import os
import time
import json
from datetime import datetime, timedelta
from typing import Optional
import jwt

# Use env var or a dev default
SECRET_KEY = os.environ.get("JWT_SECRET", "dev-secret-change-me")
ALGO = "HS256"
TTL_SECONDS = 3600  # 1 hour

# User data file for persistent storage
_USERS_FILE = "server_data/users.json"

# Admin users (always available, hardcoded)
_ADMIN_USERS = {
    "admin": "admin123",
    "user": "1234",
    "jenil": "1234"
}

def _load_users():
    """Load users from file, merge with admin users"""
    users = _ADMIN_USERS.copy()
    
    if os.path.exists(_USERS_FILE):
        try:
            with open(_USERS_FILE, 'r', encoding='utf-8') as f:
                file_users = json.load(f)
                users.update(file_users)
                print(f"[Auth] Loaded {len(file_users)} registered users from file")
        except json.JSONDecodeError:
            print(f"[Auth] Invalid JSON in users file, skipping")
        except Exception as e:
            print(f"[Auth] Error loading users: {e}")
    
    return users


def _save_users(users):
    """Save users to file (excluding admin users)"""
    try:
        os.makedirs(os.path.dirname(_USERS_FILE), exist_ok=True)
        
        # Only save non-admin users (keep admin users hardcoded)
        file_users = {k: v for k, v in users.items() if k not in _ADMIN_USERS}
        
        with open(_USERS_FILE, 'w', encoding='utf-8') as f:
            json.dump(file_users, f, indent=2)
        
        print(f"[Auth] Saved {len(file_users)} registered users to file")
    except Exception as e:
        print(f"[Auth] Error saving users: {e}")


def authenticate_user(username: str, password: str) -> bool:
    """Authenticate user - checks both admin and registered users
    
    Args:
        username: Username to authenticate
        password: Password to verify
    
    Returns:
        True if credentials are valid, False otherwise
    """
    if not username or not password:
        return False
    
    users = _load_users()
    
    # Check if user exists and password matches
    is_valid = users.get(username) == password
    
    if is_valid:
        print(f"[Auth] User '{username}' authenticated successfully")
    else:
        print(f"[Auth] Authentication failed for user '{username}'")
    
    return is_valid


def register_user(username: str, password: str) -> tuple[bool, str]:
    """Register a new user
    
    Args:
        username: New username (must not exist)
        password: User password (must not be empty)
    
    Returns:
        Tuple of (success: bool, message: str)
        - (True, "User registered successfully") if successful
        - (False, "Username already exists") if username taken
        - (False, "Invalid username or password") if invalid input
    """
    # Validate input
    if not username or not password:
        return False, "Invalid username or password"
    
    if len(username) < 3:
        return False, "Username must be at least 3 characters"
    
    if len(password) < 4:
        return False, "Password must be at least 4 characters"
    
    users = _load_users()
    
    # Check if user already exists
    if username in users:
        print(f"[Auth] Registration failed: username '{username}' already exists")
        return False, "Username already exists"
    
    # Add new user and save
    users[username] = password
    _save_users(users)
    
    print(f"[Auth] User '{username}' registered successfully")
    return True, "User registered successfully"


def generate_token(username: str) -> str:
    """Generate JWT token for authenticated user
    
    Args:
        username: Username to encode in token
    
    Returns:
        JWT token string
    """
    payload = {
        "username": username,
        "exp": datetime.utcnow() + timedelta(hours=24)
    }
    
    token = jwt.encode(payload, SECRET_KEY, algorithm="HS256")
    print(f"[Auth] Generated token for user '{username}' (expires in 24h)")
    
    return token


def verify_token(token: str) -> Optional[str]:
    """Verify JWT token and extract username
    
    Args:
        token: JWT token string to verify
    
    Returns:
        Username if token is valid, None otherwise
    """
    if not token:
        return None
    
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
        username = payload.get("username")
        
        if username:
            print(f"[Auth] Token verified for user '{username}'")
        
        return username
    
    except jwt.ExpiredSignatureError:
        print(f"[Auth] Token has expired")
        return None
    except jwt.InvalidTokenError as e:
        print(f"[Auth] Invalid token: {e}")
        return None
    except Exception as e:
        print(f"[Auth] Token verification error: {e}")
        return None


def get_all_users() -> dict:
    """Get all registered users (admin + file-based)
    
    ⚠️ For admin purposes only - does NOT return passwords in production
    
    Returns:
        Dictionary of username -> user_type
    """
    users = _load_users()
    
    result = {}
    for username in users:
        if username in _ADMIN_USERS:
            result[username] = "admin"
        else:
            result[username] = "registered"
    
    return result


def delete_user(username: str) -> tuple[bool, str]:
    """Delete a registered user (cannot delete admin users)
    
    Args:
        username: Username to delete
    
    Returns:
        Tuple of (success: bool, message: str)
    """
    if username in _ADMIN_USERS:
        return False, "Cannot delete admin users"
    
    users = _load_users()
    
    if username not in users:
        return False, "User not found"
    
    del users[username]
    _save_users(users)
    
    print(f"[Auth] ✅ User '{username}' deleted")
    return True, "User deleted successfully"


def change_password(username: str, old_password: str, new_password: str) -> tuple[bool, str]:
    """Change user password
    
    Args:
        username: Username
        old_password: Current password (must be correct)
        new_password: New password (must be different)
    
    Returns:
        Tuple of (success: bool, message: str)
    """
    if not authenticate_user(username, old_password):
        return False, "Incorrect current password"
    
    if len(new_password) < 4:
        return False, "New password must be at least 4 characters"
    
    if new_password == old_password:
        return False, "New password must be different from current password"
    
    users = _load_users()
    users[username] = new_password
    _save_users(users)
    
    print(f"[Auth] Password changed for user '{username}'")
    return True, "Password changed successfully"