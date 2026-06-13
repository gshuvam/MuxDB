import { MigrationError } from './errors';

export class PIDController {
  public kp: number;
  public ki: number;
  public kd: number;
  public targetDeviation: number;
  
  private integral: number = 0;
  private lastError: number = 0;
  private lastTime: number = Date.now();
  public currentDelay: number = 0.01;

  constructor(
    kp: number = 0.5,
    ki: number = 0.1,
    kd: number = 0.2,
    targetDeviation: number = 0.1
  ) {
    this.kp = kp;
    this.ki = ki;
    this.kd = kd;
    this.targetDeviation = targetDeviation;
  }

  public update(currentDeviation: number): number {
    const now = Date.now();
    let dt = (now - this.lastTime) / 1000;
    if (dt <= 0) {
      dt = 0.001;
    }

    const error = currentDeviation - this.targetDeviation;
    this.integral += error * dt;
    const derivative = (error - this.lastError) / dt;

    const output = this.kp * error + this.ki * this.integral + this.kd * derivative;

    this.lastError = error;
    this.lastTime = now;

    this.currentDelay = Math.max(0, this.currentDelay + output * 0.01);
    return this.currentDelay;
  }
}

export class LiveMigrator {
  public pid = new PIDController();
  private activeMigrations: Map<string, {
    source: string;
    destination: string;
    pendingKeys: Set<string>;
    migratedKeys: Set<string>;
    dataStore: Map<string, any>;
  }> = new Map();
  private keyMigrationMap: Map<string, string> = new Map();

  constructor(
    public readonly driver?: any,
    public readonly telemetry?: any
  ) {}

  public startMigration(
    migrationId: string,
    sourceShardId: string,
    destShardId: string,
    keys: string[]
  ): void {
    if (this.activeMigrations.has(migrationId)) {
      throw new MigrationError(`Migration '${migrationId}' is already active.`);
    }

    this.activeMigrations.set(migrationId, {
      source: sourceShardId,
      destination: destShardId,
      pendingKeys: new Set(keys),
      migratedKeys: new Set(),
      dataStore: new Map(),
    });

    for (const key of keys) {
      this.keyMigrationMap.set(key, migrationId);
    }
  }

  public isMigratingKey(key: string): boolean {
    return this.keyMigrationMap.has(key);
  }

  public onDemandPull(key: string): any {
    /** Pull and write key immediately on-demand. */
    const migId = this.keyMigrationMap.get(key);
    if (!migId) return null;

    const mig = this.activeMigrations.get(migId)!;
    if (mig.migratedKeys.has(key)) {
      return null;
    }

    const val = mig.dataStore.get(key) || `value_of_${key}`;
    mig.migratedKeys.add(key);
    mig.pendingKeys.delete(key);

    return val;
  }

  public async coldPushStep(migrationId: string, batchSize: number = 5): Promise<boolean> {
    /** Background task pushing key data sequences dynamically. */
    const mig = this.activeMigrations.get(migrationId);
    if (!mig || mig.pendingKeys.size === 0) {
      return false;
    }

    const batch = Array.from(mig.pendingKeys).slice(0, batchSize);
    for (const key of batch) {
      mig.migratedKeys.add(key);
      mig.pendingKeys.delete(key);
    }

    // Delay using PID delay value
    await new Promise((resolve) => setTimeout(resolve, this.pid.currentDelay * 1000));
    return mig.pendingKeys.size > 0;
  }

  public adjustThrottling(currentLatency: number, targetLatency: number): number {
    const deviation = (currentLatency - targetLatency) / Math.max(0.001, targetLatency);
    return this.pid.update(deviation);
  }
}
