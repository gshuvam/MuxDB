'use client';

import { useState, useEffect } from 'react';

interface Shard {
  id: string;
  backend: string;
  host: string;
  port: number;
  database: string;
  weight: number;
  health: 'healthy' | 'slow' | 'failed';
  qps: number;
  latency: number;
  hotKeys: string[];
}

interface Migration {
  id: string;
  source: string;
  dest: string;
  totalKeys: number;
  migratedKeys: number;
  status: 'running' | 'completed';
}

interface ClusterData {
  shards: Shard[];
  migrations: Migration[];
  tenants: Record<string, string>;
  p50Latency: number;
  p99Latency: number;
  totalQps: number;
  imbalanceRatio: number;
}

export default function Home() {
  const [data, setData] = useState<ClusterData | null>(null);
  const [migId, setMigId] = useState('mig-101');
  const [migSrc, setMigSrc] = useState('shard-0');
  const [migDst, setMigDst] = useState('shard-1');
  const [migKeys, setMigKeys] = useState('120');

  const [selTenant, setSelTenant] = useState('tenant-alpha');
  const [selTenantDst, setSelTenantDst] = useState('shard-2');

  const fetchTelemetry = async () => {
    try {
      const res = await fetch('/api/telemetry');
      const json = await res.json();
      setData(json);
    } catch (err) {
      console.error('Failed to fetch telemetry', err);
    }
  };

  useEffect(() => {
    fetchTelemetry();
    const interval = setInterval(fetchTelemetry, 1500);
    return () => clearInterval(interval);
  }, []);

  const triggerAction = async (action: string, target: string, parameter?: any) => {
    try {
      await fetch('/api/telemetry', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ action, target, parameter }),
      });
      fetchTelemetry();
    } catch (err) {
      console.error('Action failed', err);
    }
  };

  if (!data) {
    return (
      <div style={{ display: 'flex', justifyContent: 'center', alignItems: 'center', height: '100vh', fontSize: '20px' }}>
        Loading MuxDB Autonomous Dashboard...
      </div>
    );
  }

  return (
    <div>
      <header>
        <div className="logo">
          <span>⬢</span> MuxDB Control Center
        </div>
        <div style={{ display: 'flex', alignItems: 'center', gap: '12px' }}>
          <div className="badge">autonomous routing active</div>
          <span className="dot green"></span>
        </div>
      </header>

      <main className="dashboard-grid">
        {/* Metric Summary Cards */}
        <div className="col-12" style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))', gap: '24px' }}>
          <div className="glass-panel" style={{ borderLeft: '4px solid var(--accent-blue)' }}>
            <div style={{ fontSize: '14px', color: 'var(--text-muted)', fontWeight: 600, textTransform: 'uppercase' }}>Total Throughput</div>
            <div style={{ fontSize: '36px', fontWeight: 700, marginTop: '8px' }}>{data.totalQps} <span style={{ fontSize: '16px', fontWeight: 400 }}>QPS</span></div>
          </div>
          <div className="glass-panel" style={{ borderLeft: '4px solid var(--accent-purple)' }}>
            <div style={{ fontSize: '14px', color: 'var(--text-muted)', fontWeight: 600, textTransform: 'uppercase' }}>Imbalance Ratio</div>
            <div style={{ fontSize: '36px', fontWeight: 700, marginTop: '8px' }}>{data.imbalanceRatio}x</div>
          </div>
          <div className="glass-panel" style={{ borderLeft: '4px solid var(--accent-green)' }}>
            <div style={{ fontSize: '14px', color: 'var(--text-muted)', fontWeight: 600, textTransform: 'uppercase' }}>Avg Latency</div>
            <div style={{ fontSize: '36px', fontWeight: 700, marginTop: '8px' }}>{data.p50Latency} <span style={{ fontSize: '16px', fontWeight: 400 }}>ms</span></div>
          </div>
          <div className="glass-panel" style={{ borderLeft: '4px solid var(--accent-yellow)' }}>
            <div style={{ fontSize: '14px', color: 'var(--text-muted)', fontWeight: 600, textTransform: 'uppercase' }}>P99 Latency</div>
            <div style={{ fontSize: '36px', fontWeight: 700, marginTop: '8px' }}>{data.p99Latency} <span style={{ fontSize: '16px', fontWeight: 400 }}>ms</span></div>
          </div>
        </div>

        {/* Shard Topology Map */}
        <div className="col-8 glass-panel">
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '24px' }}>
            <h2 style={{ fontSize: '20px', fontWeight: 600 }}>Shard Topology Map</h2>
            <button onClick={() => triggerAction('ADD_NODE', '')}>+ Add Shard Node</button>
          </div>

          <div className="shard-grid">
            {data.shards.map((shard) => (
              <div key={shard.id} className={`shard-card ${shard.health}`}>
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '12px' }}>
                  <div style={{ fontWeight: 700, fontSize: '16px' }}>{shard.id}</div>
                  <span className={`dot ${shard.health === 'healthy' ? 'green' : shard.health === 'slow' ? 'yellow' : 'red'}`}></span>
                </div>
                <div style={{ fontSize: '12px', color: 'var(--text-muted)', fontFamily: 'var(--font-mono)', marginBottom: '16px' }}>
                  {shard.host}:{shard.port}<br />
                  db: {shard.database}
                </div>

                <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: '13px', margin: '8px 0' }}>
                  <span>Load:</span>
                  <span style={{ fontWeight: 600 }}>{shard.qps} QPS</span>
                </div>
                <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: '13px', margin: '8px 0' }}>
                  <span>Latency:</span>
                  <span style={{ fontWeight: 600 }}>{shard.latency} ms</span>
                </div>

                {shard.hotKeys.length > 0 && (
                  <div style={{ marginTop: '12px', background: 'rgba(245, 158, 11, 0.1)', border: '1px solid rgba(245, 158, 11, 0.2)', padding: '6px', borderRadius: '6px', fontSize: '11px' }}>
                    <div style={{ color: 'var(--accent-yellow)', fontWeight: 600 }}>🔥 Hot Keys Deteted:</div>
                    <div style={{ fontFamily: 'var(--font-mono)' }}>{shard.hotKeys.join(', ')}</div>
                  </div>
                )}

                {/* Shard Controls */}
                <div style={{ display: 'flex', gap: '8px', marginTop: '16px' }}>
                  {shard.health === 'healthy' ? (
                    <>
                      <button className="secondary" style={{ flex: 1, padding: '6px', fontSize: '11px', color: 'var(--accent-yellow)' }} onClick={() => triggerAction('SLOW_NODE', shard.id)}>Slow</button>
                      <button className="secondary" style={{ flex: 1, padding: '6px', fontSize: '11px', color: 'var(--accent-red)' }} onClick={() => triggerAction('FAIL_NODE', shard.id)}>Fail</button>
                    </>
                  ) : (
                    <button className="secondary" style={{ width: '100%', padding: '6px', fontSize: '11px', color: 'var(--accent-green)' }} onClick={() => triggerAction('RECOVER_NODE', shard.id)}>Recover Node</button>
                  )}
                </div>
              </div>
            ))}
          </div>
        </div>

        {/* Controls & Operations Side Panel */}
        <div className="col-4" style={{ display: 'flex', flexDirection: 'column', gap: '24px' }}>
          
          {/* Live Migration Trigger */}
          <div className="glass-panel">
            <h2 style={{ fontSize: '18px', fontWeight: 600, marginBottom: '16px' }}>Trigger Live Shard Migration</h2>
            <div style={{ display: 'flex', flexDirection: 'column', gap: '12px' }}>
              <div style={{ display: 'flex', flexDirection: 'column', gap: '4px' }}>
                <label style={{ fontSize: '12px', color: 'var(--text-muted)' }}>Migration ID</label>
                <input type="text" value={migId} onChange={(e) => setMigId(e.target.value)} />
              </div>
              <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '12px' }}>
                <div style={{ display: 'flex', flexDirection: 'column', gap: '4px' }}>
                  <label style={{ fontSize: '12px', color: 'var(--text-muted)' }}>Source Shard</label>
                  <select value={migSrc} onChange={(e) => setMigSrc(e.target.value)}>
                    {data.shards.map((s) => <option key={s.id} value={s.id}>{s.id}</option>)}
                  </select>
                </div>
                <div style={{ display: 'flex', flexDirection: 'column', gap: '4px' }}>
                  <label style={{ fontSize: '12px', color: 'var(--text-muted)' }}>Dest Shard</label>
                  <select value={migDst} onChange={(e) => setMigDst(e.target.value)}>
                    {data.shards.map((s) => <option key={s.id} value={s.id}>{s.id}</option>)}
                  </select>
                </div>
              </div>
              <div style={{ display: 'flex', flexDirection: 'column', gap: '4px' }}>
                <label style={{ fontSize: '12px', color: 'var(--text-muted)' }}>Number of Keys</label>
                <input type="number" value={migKeys} onChange={(e) => setMigKeys(e.target.value)} />
              </div>
              <button style={{ marginTop: '8px' }} onClick={() => triggerAction('TRIGGER_MIGRATION', '', { id: migId, source: migSrc, dest: migDst, totalKeys: parseInt(migKeys) })}>
                Execute Zephyr Migration
              </button>
            </div>
          </div>

          {/* Tenant relocation */}
          <div className="glass-panel">
            <h2 style={{ fontSize: '18px', fontWeight: 600, marginBottom: '16px' }}>Tenant Routing Override</h2>
            <div style={{ display: 'flex', flexDirection: 'column', gap: '12px' }}>
              <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '12px' }}>
                <div style={{ display: 'flex', flexDirection: 'column', gap: '4px' }}>
                  <label style={{ fontSize: '12px', color: 'var(--text-muted)' }}>Tenant</label>
                  <select value={selTenant} onChange={(e) => setSelTenant(e.target.value)}>
                    {Object.keys(data.tenants).map((t) => <option key={t} value={t}>{t}</option>)}
                  </select>
                </div>
                <div style={{ display: 'flex', flexDirection: 'column', gap: '4px' }}>
                  <label style={{ fontSize: '12px', color: 'var(--text-muted)' }}>Destination</label>
                  <select value={selTenantDst} onChange={(e) => setSelTenantDst(e.target.value)}>
                    {data.shards.map((s) => <option key={s.id} value={s.id}>{s.id}</option>)}
                  </select>
                </div>
              </div>
              <button style={{ marginTop: '8px' }} onClick={() => triggerAction('MOVE_TENANT', '', { tenantId: selTenant, destShard: selTenantDst })}>
                Relocate Tenant Group
              </button>

              <div style={{ marginTop: '16px' }}>
                <div style={{ fontSize: '12px', color: 'var(--text-muted)', marginBottom: '8px' }}>Active Tenant Mappings:</div>
                {Object.entries(data.tenants).map(([tenant, shard]) => (
                  <div key={tenant} style={{ display: 'flex', justifyContent: 'space-between', fontSize: '13px', padding: '6px 0', borderBottom: '1px solid rgba(255,255,255,0.04)' }}>
                    <span style={{ fontWeight: 600 }}>{tenant}</span>
                    <span style={{ color: 'var(--accent-purple)', fontFamily: 'var(--font-mono)' }}>➔ {shard}</span>
                  </div>
                ))}
              </div>
            </div>
          </div>

          {/* Active migrations list */}
          {data.migrations.length > 0 && (
            <div className="glass-panel">
              <h2 style={{ fontSize: '18px', fontWeight: 600, marginBottom: '16px' }}>Active Migrations Progress</h2>
              <div style={{ display: 'flex', flexDirection: 'column', gap: '16px' }}>
                {data.migrations.map((m) => {
                  const pct = Math.floor((m.migratedKeys / m.totalKeys) * 100);
                  return (
                    <div key={m.id} style={{ background: 'rgba(255,255,255,0.01)', border: '1px solid var(--card-border)', padding: '12px', borderRadius: '10px' }}>
                      <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: '13px', fontWeight: 600, marginBottom: '4px' }}>
                        <span>{m.id}</span>
                        <span style={{ color: m.status === 'completed' ? 'var(--accent-green)' : 'var(--accent-blue)' }}>
                          {m.status === 'completed' ? 'Completed' : `${pct}%`}
                        </span>
                      </div>
                      <div style={{ fontSize: '11px', color: 'var(--text-muted)', marginBottom: '8px' }}>
                        {m.source} ➔ {m.dest} ({m.migratedKeys}/{m.totalKeys} keys)
                      </div>
                      <div className="progress-bar-container">
                        <div className="progress-bar-fill" style={{ width: `${pct}%` }}></div>
                      </div>
                    </div>
                  );
                })}
              </div>
            </div>
          )}

        </div>
      </main>
    </div>
  );
}
