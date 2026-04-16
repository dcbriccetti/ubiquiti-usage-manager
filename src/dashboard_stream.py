'''Dashboard Server-Sent Events (SSE) stream helpers.

This module provides the streaming loop used by `/dashboard-stream`.
It rebuilds dashboard data at a fixed interval and emits JSON payloads as
SSE frames (`data: ...`) so connected browsers can update in near real time.

Why separate this from app.py:
- route wiring stays lightweight
- stream cadence/serialization concerns are isolated
- easier to test stream behavior independently
'''

import json
import time
from collections.abc import Iterator

from dashboard_service import WindowName, build_dashboard_data, build_dashboard_payload


def event_stream(window_name: WindowName, live_update_seconds: int) -> Iterator[str]:
    'Yield SSE frames at a fixed cadence using the selected dashboard window.'
    while True:
        # Rebuild on each tick so clients always receive a fresh snapshot from current state.
        data = build_dashboard_data(window_name, live_update_seconds)
        payload = build_dashboard_payload(data)
        yield f'data: {json.dumps(payload)}\n\n'
        time.sleep(live_update_seconds)
