import { logger } from '../observability';

export function logAuditEvent(
  actor: string,
  action: string,
  target: string,
  status: string = 'success',
  details: Record<string, any> = {}
): void {
  /** Log a structured configuration or admin audit event to the structured logger. */
  logger.info({
    timestamp: Math.floor(Date.now() / 1000),
    actor,
    action,
    target,
    status,
    details,
  }, 'audit_event');
}
