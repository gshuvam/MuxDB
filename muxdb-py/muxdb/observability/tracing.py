from __future__ import annotations

import contextlib
from typing import Any, Dict, Generator, Optional

from opentelemetry import trace
from opentelemetry.trace import Status, StatusCode

def get_tracer() -> trace.Tracer:
    """Get the standard OpenTelemetry Tracer for MuxDB."""
    return trace.get_tracer("muxdb")

@contextlib.contextmanager
def trace_span(
    name: str,
    attributes: Optional[Dict[str, Any]] = None,
) -> Generator[trace.Span, None, None]:
    """Context manager to execute a block of code within a trace span.

    Automatically handles exception recording and status settings.
    """
    tracer = get_tracer()
    with tracer.start_as_current_span(name) as span:
        if attributes:
            for k, v in attributes.items():
                if v is not None:
                    span.set_attribute(k, str(v) if not isinstance(v, (int, float, bool)) else v)
        try:
            yield span
            span.set_status(Status(StatusCode.OK))
        except Exception as e:
            span.set_status(Status(StatusCode.ERROR, str(e)))
            span.record_exception(e)
            raise
