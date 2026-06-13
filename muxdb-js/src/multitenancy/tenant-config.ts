export class TenantConfig {
  private overrides: Map<string, Map<string, any>> = new Map();

  constructor(initialOverrides?: Record<string, Record<string, any>>) {
    if (initialOverrides) {
      for (const [tenantId, configObj] of Object.entries(initialOverrides)) {
        const tenantMap = new Map(Object.entries(configObj));
        this.overrides.set(tenantId, tenantMap);
      }
    }
  }

  public setOverride(tenantId: string, key: string, value: any): void {
    let tenantMap = this.overrides.get(tenantId);
    if (!tenantMap) {
      tenantMap = new Map();
      this.overrides.set(tenantId, tenantMap);
    }
    tenantMap.set(key, value);
  }

  public getOverride(tenantId: string, key: string, defaultValue?: any): any {
    const tenantMap = this.overrides.get(tenantId);
    if (tenantMap && tenantMap.has(key)) {
      return tenantMap.get(key);
    }
    return defaultValue;
  }
}
