import { describe, it, expect, vi } from 'vitest';
import * as fs from 'fs';
import {
  MuxDBError,
  RoutingError,
  SecurityError,
  BulkheadLimitExceeded,
  CircuitOpenError,
  MigrationError,
  getCorrelationId,
  runWithCorrelationId,
  logger,
  queriesTotal,
  traceSpan,
  validateApiKey,
  generateToken,
  verifyToken,
  Role,
  hasPermission,
  requireRole,
  logAuditEvent,
  SecretsLoader,
  CircuitBreaker,
  State as CBState,
  retryWithBackoff,
  TokenBucketRateLimiter,
  Bulkhead,
  TenantRouter,
  TenantConfig,
  TenantMigrator,
} from '../src/index';


describe('Node.js Enterprise Infrastructure Smoke Tests', () => {

  // --- Observability Tests ---

  it('should manage correlation ID using AsyncLocalStorage context', () => {
    expect(getCorrelationId()).toBe('');
    
    runWithCorrelationId('my-custom-cid', () => {
      expect(getCorrelationId()).toBe('my-custom-cid');
    });
    
    expect(getCorrelationId()).toBe('');
  });

  it('should define and export OpenTelemetry metrics', () => {
    expect(queriesTotal).toBeDefined();
  });

  it('should execute functions within traceSpan', async () => {
    const result = await traceSpan('test-span', { key: 'value' }, async (span) => {
      expect(span).toBeDefined();
      return 'success';
    });
    expect(result).toBe('success');
  });

  // --- Security Tests ---

  it('should securely compare API keys', () => {
    expect(validateApiKey('correct-key', 'correct-key')).toBe(true);
    expect(validateApiKey('wrong-key', 'correct-key')).toBe(false);
  });

  it('should sign and verify JWT tokens securely', () => {
    const payload = { userId: 123, role: 'admin' };
    const secret = 'test-secret';
    
    const token = generateToken(payload, secret, 10);
    const decoded = verifyToken(token, secret);
    expect(decoded).not.toBeNull();
    expect(decoded!.userId).toBe(123);
    
    // Test expired token
    const expiredToken = generateToken(payload, secret, -10);
    expect(verifyToken(expiredToken, secret)).toBeNull();
    
    // Test tampered token
    expect(verifyToken(token + 'tamper', secret)).toBeNull();
  });

  it('should enforce RBAC role permissions', () => {
    expect(hasPermission(Role.ADMIN, Role.OPERATOR)).toBe(true);
    expect(hasPermission(Role.OPERATOR, Role.VIEWER)).toBe(true);
    expect(hasPermission(Role.VIEWER, Role.ADMIN)).toBe(false);
    expect(hasPermission('operator', 'admin')).toBe(false);
  });

  it('should enforce requireRole decorator permissions', () => {
    class TestClass {
      public activeRole: Role;
      constructor(role: Role) {
        this.activeRole = role;
      }

      @requireRole(Role.ADMIN)
      public adminMethod(): string {
        return 'admin';
      }

      @requireRole(Role.OPERATOR)
      public operatorMethod(): string {
        return 'operator';
      }
    }

    const adminInst = new TestClass(Role.ADMIN);
    const opInst = new TestClass(Role.OPERATOR);
    const viewInst = new TestClass(Role.VIEWER);

    expect(adminInst.adminMethod()).toBe('admin');
    expect(opInst.operatorMethod()).toBe('operator');
    
    expect(() => opInst.adminMethod()).toThrow(SecurityError);
    expect(() => viewInst.operatorMethod()).toThrow(SecurityError);
  });

  it('should resolve credentials using SecretsLoader', () => {
    const loader = new SecretsLoader();
    
    process.env.TEST_LOADER_DB_PASS = 'super-secret';
    expect(loader.resolve('env:TEST_LOADER_DB_PASS')).toBe('super-secret');
    
    expect(loader.resolve('vault:some/secret#key')).toBe('mock-vault-resolved-some-secret-key');
    expect(loader.resolve('aws:secret#key')).toBe('mock-aws-resolved-secret-key');
    expect(loader.resolve('plain_pass')).toBe('plain_pass');
  });

  // --- Resilience Tests ---

  it('should enforce Circuit Breaker state transitions', async () => {
    const cb = new CircuitBreaker('shard-0', 2, 50); // 50ms recovery
    
    // Healthy CLOSED call
    const val = await cb.call(async () => 'healthy');
    expect(val).toBe('healthy');
    
    // Triggering failures to trip circuit
    await expect(cb.call(async () => { throw new Error('fail'); })).rejects.toThrow('fail');
    await expect(cb.call(async () => { throw new Error('fail'); })).rejects.toThrow('fail');
    
    // Circuit should be OPEN now
    await expect(cb.call(async () => 'wont-execute')).rejects.toThrow(CircuitOpenError);
    
    // Wait for recovery timeout
    await new Promise((resolve) => setTimeout(resolve, 60));
    
    // Circuit should be in HALF_OPEN and transition back to CLOSED upon success
    const recoveredVal = await cb.call(async () => 'success');
    expect(recoveredVal).toBe('success');
  });

  it('should perform retry with exponential backoff and succeed', async () => {
    let attempts = 0;
    const task = async () => {
      attempts++;
      if (attempts < 3) {
        throw new Error('transient');
      }
      return 'success';
    };

    const res = await retryWithBackoff(task, 3, 5, 20, 2);
    expect(res).toBe('success');
    expect(attempts).toBe(3);
  });

  it('should enforce TokenBucketRateLimiter rules', () => {
    const limiter = new TokenBucketRateLimiter(10, 2);
    expect(limiter.acquire(1)).toBe(true);
    expect(limiter.acquire(1)).toBe(true);
    expect(limiter.acquire(1)).toBe(false);
  });

  it('should enforce Bulkhead isolation constraints', async () => {
    const bulkhead = new Bulkhead(2);
    
    const blockTask = () => new Promise<string>((resolve) => setTimeout(() => resolve('ok'), 20));
    
    const p1 = bulkhead.call(blockTask);
    const p2 = bulkhead.call(blockTask);
    
    // Third one should immediately throw BulkheadLimitExceeded since maxConcurrency is 2
    await expect(bulkhead.call(blockTask)).rejects.toThrow(BulkheadLimitExceeded);
    
    await Promise.all([p1, p2]);
  });

  // --- Multi-Tenancy Tests ---

  it('should resolve tenant shard routing and config overrides', () => {
    const router = new TenantRouter();
    router.registerTenant('tenant-1', ['s0', 's1']);
    expect(router.resolveTenantShards('tenant-1')).toEqual(['s0', 's1']);
    expect(() => router.resolveTenantShards('tenant-2')).toThrow(RoutingError);

    const config = new TenantConfig();
    config.setOverride('tenant-1', 'pool.maxSize', 15);
    expect(config.getOverride('tenant-1', 'pool.maxSize')).toBe(15);
    expect(config.getOverride('tenant-2', 'pool.maxSize')).toBeUndefined();
  });

  it('should orchestrate tenant migration and trigger atomic routing swap', async () => {
    const router = new TenantRouter();
    router.registerTenant('tenant-migrating', ['s0']);
    
    const migrator = new TenantMigrator(router);
    await migrator.migrateTenant('tenant-migrating', ['s1', 's2']);
    
    expect(router.resolveTenantShards('tenant-migrating')).toEqual(['s1', 's2']);
  });

});
