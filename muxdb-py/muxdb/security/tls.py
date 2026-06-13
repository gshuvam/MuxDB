from __future__ import annotations

import ssl
from typing import Optional

def create_client_ssl_context(
    ca_cert_path: Optional[str] = None,
    cert_chain_path: Optional[str] = None,
    private_key_path: Optional[str] = None,
) -> ssl.SSLContext:
    """Create an SSLContext for a secure client connection (e.g. gRPC or DB).

    Optionally configures mTLS if cert_chain_path and private_key_path are provided.
    """
    context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
    
    if ca_cert_path:
        context.load_verify_locations(ca_cert_path)
    
    if cert_chain_path and private_key_path:
        context.load_cert_chain(certfile=cert_chain_path, keyfile=private_key_path)
        
    return context

def create_server_ssl_context(
    cert_chain_path: str,
    private_key_path: str,
    ca_cert_path: Optional[str] = None,
    require_client_auth: bool = False,
) -> ssl.SSLContext:
    """Create an SSLContext for a secure server.

    If require_client_auth is True, mTLS is enforced and ca_cert_path must be provided.
    """
    context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
    context.load_cert_chain(certfile=cert_chain_path, keyfile=private_key_path)
    
    if require_client_auth:
        if not ca_cert_path:
            raise ValueError("ca_cert_path is required when require_client_auth is True")
        context.load_verify_locations(ca_cert_path)
        context.verify_mode = ssl.CERT_REQUIRED
        
    return context
