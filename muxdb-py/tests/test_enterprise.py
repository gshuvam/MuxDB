from __future__ import annotations

import os
import time
import unittest
from unittest.mock import MagicMock

from muxdb.errors import (
    BulkheadLimitExceeded,
    CircuitOpenError,
    MigrationError,
    SecurityError,
)
from muxdb.observability import (
    get_correlation_id,
    set_correlation_id,
    setup_logging,
    queries_total,
    trace_span,
)
from muxdb.security import (
    create_client_ssl_context,
    create_server_ssl_context,
    validate_api_key,
    generate_token,
    verify_token,
    Role,
    has_permission,
    require_role,
    log_audit_event,
    SecretsLoader,
)
from muxdb.resilience import (
    CircuitBreaker,
    State as CBState,
    retry_with_backoff,
    TokenBucketRateLimiter,
    Bulkhead,
)
from muxdb.multitenancy import (
    TenantRouter,
    TenantConfig,
    TenantMigrator,
)


class TestEnterpriseInfrastructure(unittest.TestCase):

    # --- Observability Tests ---

    def test_correlation_id(self) -> None:
        cid = get_correlation_id()
        self.assertTrue(len(cid) > 0)
        set_correlation_id("test-correlation-id")
        self.assertEqual(get_correlation_id(), "test-correlation-id")

    def test_tracing_context_manager(self) -> None:
        span_executed = False
        with trace_span("test-operation", {"custom_attr": "value"}) as span:
            span_executed = True
            self.assertIsNotNone(span)
        self.assertTrue(span_executed)

    def test_metrics_definition(self) -> None:
        self.assertIsNotNone(queries_total)

    # --- Security Tests ---

    def test_tls_contexts(self) -> None:
        # Client SSLContext
        context = create_client_ssl_context()
        self.assertIsNotNone(context)

    def test_api_key_validation(self) -> None:
        self.assertTrue(validate_api_key("secret-key", "secret-key"))
        self.assertFalse(validate_api_key("wrong-key", "secret-key"))
        self.assertFalse(validate_api_key("", "secret-key"))

    def test_jwt_tokens(self) -> None:
        payload = {"user_id": 123, "role": "admin"}
        secret = "super-secret"
        
        # Valid Token
        token = generate_token(payload, secret, expires_in=10)
        decoded = verify_token(token, secret)
        self.assertIsNotNone(decoded)
        self.assertEqual(decoded["user_id"], 123)
        self.assertEqual(decoded["role"], "admin")
        
        # Expired Token
        expired_token = generate_token(payload, secret, expires_in=-10)
        decoded_expired = verify_token(expired_token, secret)
        self.assertIsNone(decoded_expired)
        
        # Tampered Token
        tampered_token = token[:-5] + "tamper"
        decoded_tampered = verify_token(tampered_token, secret)
        self.assertIsNone(decoded_tampered)

    def test_rbac_permissions(self) -> None:
        self.assertTrue(has_permission(Role.ADMIN, Role.OPERATOR))
        self.assertTrue(has_permission(Role.OPERATOR, Role.VIEWER))
        self.assertFalse(has_permission(Role.VIEWER, Role.OPERATOR))
        
        # Role mapping from string
        self.assertTrue(has_permission("admin", "operator"))
        self.assertFalse(has_permission("viewer", "admin"))

    def test_rbac_decorator(self) -> None:
        class ProtectedResource:
            def __init__(self, role: Role) -> None:
                self._active_role = role

            @require_role(Role.ADMIN)
            def admin_only_method(self) -> str:
                return "admin success"

            @require_role(Role.OPERATOR)
            def operator_method(self) -> str:
                return "operator success"

        admin_resource = ProtectedResource(Role.ADMIN)
        operator_resource = ProtectedResource(Role.OPERATOR)
        viewer_resource = ProtectedResource(Role.VIEWER)

        self.assertEqual(admin_resource.admin_only_method(), "admin success")
        self.assertEqual(operator_resource.operator_method(), "operator success")

        with self.assertRaises(SecurityError):
            operator_resource.admin_only_method()

        with self.assertRaises(SecurityError):
            viewer_resource.operator_method()

    def test_secrets_loader(self) -> None:
        loader = SecretsLoader()
        
        # Env provider
        os.environ["TEST_SECRET_DB_PASS"] = "my-secure-password"
        self.assertEqual(loader.resolve("env:TEST_SECRET_DB_PASS"), "my-secure-password")
        
        # Vault mock provider
        self.assertEqual(loader.resolve("vault:db/password"), "mock-vault-resolved-db-password")
        
        # AWS mock provider
        self.assertEqual(loader.resolve("aws:db-pass"), "mock-aws-resolved-db-pass")
        
        # Literal secret
        self.assertEqual(loader.resolve("plain_password"), "plain_password")

    # --- Resilience Tests ---

    def test_circuit_breaker(self) -> None:
        cb = CircuitBreaker("shard-1", failure_threshold=2, recovery_timeout_s=0.1)
        
        # Closed state
        self.assertEqual(cb.call(lambda: "ok"), "ok")
        self.assertEqual(cb.state, CBState.CLOSED)
        
        # Trip to OPEN
        try:
            cb.call(lambda: exec("raise(ValueError('fail'))"))
        except Exception:
            pass
            
        try:
            cb.call(lambda: exec("raise(ValueError('fail'))"))
        except Exception:
            pass
            
        self.assertEqual(cb.state, CBState.OPEN)
        
        # Verify call raises CircuitOpenError
        with self.assertRaises(CircuitOpenError):
            cb.call(lambda: "wont-execute")
            
        # Recovery window wait
        time.sleep(0.12)
        
        # Half open
        self.assertEqual(cb.call(lambda: "recovered"), "recovered")
        self.assertEqual(cb.state, CBState.CLOSED)

    def test_retry_with_backoff(self) -> None:
        counter = 0

        def failing_func():
            nonlocal counter
            counter += 1
            if counter < 3:
                raise ValueError("temporary error")
            return "success"

        result = retry_with_backoff(failing_func, max_retries=3, base_delay=0.001)
        self.assertEqual(result, "success")
        self.assertEqual(counter, 3)

    def test_rate_limiter(self) -> None:
        limiter = TokenBucketRateLimiter(rate=10, capacity=2)
        
        self.assertTrue(limiter.acquire(1.0))
        self.assertTrue(limiter.acquire(1.0))
        self.assertFalse(limiter.acquire(1.0))  # Empty bucket

    def test_bulkhead(self) -> None:
        bulkhead = Bulkhead(max_concurrency=2)
        
        # Test success under concurrency
        self.assertEqual(bulkhead.call(lambda: "yes"), "yes")
        
        # Simulate limit exceed
        def block_call():
            bulkhead.call(lambda: bulkhead.call(lambda: bulkhead.call(lambda: "blocked")))
            
        with self.assertRaises(BulkheadLimitExceeded):
            block_call()

    # --- Multi-Tenancy Tests ---

    def test_tenant_routing_and_config(self) -> None:
        router = TenantRouter()
        router.register_tenant("tenant-A", ["s0", "s1"])
        self.assertEqual(router.resolve_tenant_shards("tenant-A"), ["s0", "s1"])

        config = TenantConfig()
        config.set_override("tenant-A", "pool.max_size", 20)
        self.assertEqual(config.get_override("tenant-A", "pool.max_size"), 20)
        self.assertIsNone(config.get_override("tenant-B", "pool.max_size"))

    def test_tenant_migration(self) -> None:
        router = TenantRouter()
        router.register_tenant("tenant-X", ["s0"])
        
        migrator = TenantMigrator(router)
        migrator.migrate_tenant("tenant-X", ["s1", "s2"])
        
        self.assertEqual(router.resolve_tenant_shards("tenant-X"), ["s1", "s2"])


if __name__ == "__main__":
    unittest.main()
