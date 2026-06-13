import { AsyncLocalStorage } from 'async_hooks';
import pino from 'pino';
import crypto from 'crypto';

const correlationIdStorage = new AsyncLocalStorage<string>();

export function getCorrelationId(): string {
  const cid = correlationIdStorage.getStore();
  if (cid) return cid;
  // If not set, generate a transient one
  return '';
}

export function runWithCorrelationId<T>(cid: string, fn: () => T): T {
  return correlationIdStorage.run(cid, fn);
}

const baseLogger = pino({
  level: process.env.LOG_LEVEL || 'info',
  formatters: {
    level: (label) => ({ level: label }),
  },
  timestamp: pino.stdTimeFunctions.isoTime,
});

// A wrapper proxy that dynamically binds the active correlation_id to log outputs
export const logger = new Proxy(baseLogger, {
  get(target, prop, receiver) {
    const value = Reflect.get(target, prop, receiver);
    if (
      typeof value === 'function' &&
      ['info', 'error', 'debug', 'warn', 'fatal', 'trace'].includes(prop as string)
    ) {
      return (objOrMsg: any, msg?: string, ...args: any[]) => {
        let cid = getCorrelationId();
        if (!cid) {
          cid = crypto.randomUUID();
        }
        if (typeof objOrMsg === 'object' && objOrMsg !== null) {
          return value.call(target, { correlation_id: cid, ...objOrMsg }, msg, ...args);
        } else {
          return value.call(target, { correlation_id: cid }, objOrMsg, msg, ...args);
        }
      };
    }
    return value;
  },
}) as unknown as pino.Logger;
