import { trace, Span, SpanStatusCode } from '@opentelemetry/api';

export function getTracer() {
  return trace.getTracer('muxdb');
}

export async function traceSpan<T>(
  name: string,
  attributes: Record<string, any> | undefined,
  fn: (span: Span) => Promise<T>
): Promise<T> {
  const tracer = getTracer();
  return tracer.startActiveSpan(name, async (span) => {
    if (attributes) {
      for (const [key, value] of Object.entries(attributes)) {
        if (value !== undefined && value !== null) {
          span.setAttribute(key, typeof value === 'object' ? JSON.stringify(value) : (value as any));
        }
      }
    }
    try {
      const result = await fn(span);
      span.setStatus({ code: SpanStatusCode.OK });
      return result;
    } catch (error: any) {
      span.setStatus({ code: SpanStatusCode.ERROR, message: error.message });
      span.recordException(error);
      throw error;
    } finally {
      span.end();
    }
  });
}

