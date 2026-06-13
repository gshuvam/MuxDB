import { SecurityError } from '../errors';

export enum Role {
  VIEWER = 1,
  OPERATOR = 2,
  ADMIN = 3
}

export function roleFromStr(name: string): Role {
  const normalized = name.toUpperCase();
  if (normalized === 'VIEWER') return Role.VIEWER;
  if (normalized === 'OPERATOR') return Role.OPERATOR;
  if (normalized === 'ADMIN') return Role.ADMIN;
  throw new SecurityError(`Unknown role: ${name}`);
}

export function hasPermission(userRole: Role | string, requiredRole: Role | string): boolean {
  const uRole = typeof userRole === 'string' ? roleFromStr(userRole) : userRole;
  const rRole = typeof requiredRole === 'string' ? roleFromStr(requiredRole) : requiredRole;
  return uRole >= rRole;
}

export function requireRole(requiredRole: Role | string) {
  const rRole = typeof requiredRole === 'string' ? roleFromStr(requiredRole) : requiredRole;
  return function (target: any, propertyKey: string, descriptor: PropertyDescriptor) {
    const originalMethod = descriptor.value;
    descriptor.value = function (this: any, ...args: any[]) {
      const userRoleAttr = this._activeRole || this.activeRole || Role.VIEWER;
      const userRole = typeof userRoleAttr === 'string' ? roleFromStr(userRoleAttr) : userRoleAttr;
      if (!hasPermission(userRole, rRole)) {
        throw new SecurityError(
          `Insufficient permissions. Required role: ${Role[rRole]}, ` +
          `current role: ${Role[userRole]}`
        );
      }
      return originalMethod.apply(this, args);
    };
    return descriptor;
  };
}
