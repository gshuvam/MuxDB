from __future__ import annotations

import base64
import hashlib
import hmac
import json
import time
from typing import Any, Dict, Optional

def _base64url_encode(data: bytes) -> bytes:
    return base64.urlsafe_b64encode(data).rstrip(b"=")

def _base64url_decode(data: str) -> bytes:
    padding = "=" * (4 - (len(data) % 4))
    return base64.urlsafe_b64decode(data + padding)

def validate_api_key(key: str, expected_key: str) -> bool:
    """Securely compare an API key using constant-time comparison to prevent timing attacks."""
    if not key or not expected_key:
        return False
    return hmac.compare_digest(key.encode("utf-8"), expected_key.encode("utf-8"))

def generate_token(payload: Dict[str, Any], secret: str, expires_in: int = 3600) -> str:
    """Generate a HMAC-SHA256 based JWT token."""
    header = {"alg": "HS256", "typ": "JWT"}
    
    payload_copy = payload.copy()
    payload_copy["exp"] = int(time.time()) + expires_in
    
    header_bytes = _base64url_encode(json.dumps(header, separators=(',', ':')).encode("utf-8"))
    payload_bytes = _base64url_encode(json.dumps(payload_copy, separators=(',', ':')).encode("utf-8"))
    
    signing_input = header_bytes + b"." + payload_bytes
    signature = hmac.new(secret.encode("utf-8"), signing_input, hashlib.sha256).digest()
    signature_bytes = _base64url_encode(signature)
    
    return (signing_input + b"." + signature_bytes).decode("utf-8")

def verify_token(token: str, secret: str) -> Optional[Dict[str, Any]]:
    """Verify and decode a HMAC-SHA256 JWT token. Returns payload if valid, otherwise None."""
    try:
        parts = token.split(".")
        if len(parts) != 3:
            return None
        
        header_segment, payload_segment, signature_segment = parts
        signing_input = (header_segment + "." + payload_segment).encode("utf-8")
        
        # Verify signature in constant time
        expected_signature = hmac.new(secret.encode("utf-8"), signing_input, hashlib.sha256).digest()
        actual_signature = _base64url_decode(signature_segment)
        
        if not hmac.compare_digest(expected_signature, actual_signature):
            return None
        
        # Decode payload
        payload_bytes = _base64url_decode(payload_segment)
        payload = json.loads(payload_bytes.decode("utf-8"))
        
        # Check expiration
        exp = payload.get("exp")
        if exp is not None and exp < time.time():
            return None
            
        return payload
    except Exception:
        return None
