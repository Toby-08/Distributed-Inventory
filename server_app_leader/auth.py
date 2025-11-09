import os
import time
from typing import Optional
import jwt

# Use env var or a dev default
SECRET_KEY = os.environ.get("JWT_SECRET", "dev-secret-change-me")
ALGO = "HS256"
TTL_SECONDS = 3600  # 1 hour

# Simple demo user store
_USERS = {
    "admin": "admin123",
    "user": "1234",
    "jenil": "1234"
}

def authenticate_user(username: str, password: str) -> bool:
    return _USERS.get(username) == password

def generate_token(username: str) -> str:
    now = int(time.time())
    payload = {"sub": username, "iat": now, "exp": now + TTL_SECONDS}
    return jwt.encode(payload, SECRET_KEY, algorithm=ALGO)

def verify_token(token: str) -> Optional[str]:
    try:
        data = jwt.decode(token, SECRET_KEY, algorithms=[ALGO])
        return data.get("sub")
    except Exception:
        return None