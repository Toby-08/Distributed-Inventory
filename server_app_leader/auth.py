import jwt
from datetime import datetime, timezone, timedelta
from typing import Optional

SECRET_KEY = "your-secret-key-change-in-production"  # TODO: Move to environment variable
ALGORITHM = "HS256"
TOKEN_EXPIRY_HOURS = 1

def generate_token(username: str) -> str:
    """Generate JWT token for authenticated user"""
    now = datetime.now(timezone.utc)
    payload = {
        "username": username,
        "exp": now + timedelta(hours=TOKEN_EXPIRY_HOURS),
        "iat": now
    }
    return jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)

def verify_token(token: str) -> str:
    """
    Returns username if valid, raises ValueError otherwise
    
    Raises:
        ValueError: If token is expired or invalid
    """
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return payload["username"]
    except jwt.ExpiredSignatureError:
        raise ValueError("Token expired")
    except jwt.InvalidTokenError:
        raise ValueError("Invalid token")