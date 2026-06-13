from __future__ import annotations

import os
from typing import Callable, Dict

class SecretsLoader:
    """Loads and resolves database secrets/credentials dynamically.

    Supports environment variables (e.g. `env:DB_PASS`), and custom resolvers
    for external secret managers like HashiCorp Vault or AWS Secrets Manager.
    """

    def __init__(self) -> None:
        self._providers: Dict[str, Callable[[str], str]] = {}
        # Register default environment provider
        self.register_provider("env", lambda key: os.getenv(key, ""))

    def register_provider(self, scheme: str, provider: Callable[[str], str]) -> None:
        """Register a custom secret provider for a URI scheme (e.g. 'vault', 'aws')."""
        self._providers[scheme.lower()] = provider

    def resolve(self, secret_ref: str) -> str:
        """Resolve a secret reference string.

        If it matches a registered scheme (e.g., 'env:PASSWORD', 'vault:secret/db#pass'),
        the corresponding provider is called to fetch the secret.
        Otherwise, returns the string literally.
        """
        if not secret_ref:
            return ""

        if ":" not in secret_ref:
            return secret_ref

        scheme, reference = secret_ref.split(":", 1)
        scheme = scheme.lower()

        if scheme in self._providers:
            return self._providers[scheme](reference)

        # Placeholders for AWS/Vault if no custom provider is registered
        if scheme == "vault":
            # e.g. vault:path/to/secret#key
            return f"mock-vault-resolved-{reference.replace('/', '-').replace('#', '-')}"
        if scheme == "aws":
            # e.g. aws:secret-id#key
            return f"mock-aws-resolved-{reference.replace('#', '-')}"

        # If scheme is unrecognized, return reference as literal
        return secret_ref
