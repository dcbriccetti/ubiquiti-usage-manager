'''Dashboard Server-Sent Events (SSE) stream helpers.

This module provides the streaming loop used by `/api/dashboard-stream`.
It rebuilds dashboard data at a fixed interval and emits JSON payloads as
SSE frames (`data: ...`) so connected browsers can update in near real time.
'''

import json
import time
from collections.abc import Iterator

from dashboard_service import ActivitySpan, WindowName, build_dashboard_data, build_dashboard_payload


def sleep_until_next_boundary(interval_seconds: int, offset_seconds: int = 0) -> None:
    'Sleep until the next wall-clock interval boundary plus optional offset.'
    if interval_seconds <= 0:
        return

    now = time.time()
    next_boundary = (int(now // interval_seconds) + 1) * interval_seconds
    next_tick = next_boundary + max(0, offset_seconds)
    if next_tick <= now:
        next_tick += interval_seconds
    time.sleep(max(0.0, next_tick - now))


def event_stream(
    window_name: WindowName,
    activity_span: ActivitySpan,
    live_update_seconds: int,
    boundary_offset_seconds: int = 0,
) -> Iterator[str]:
    'Yield SSE frames at a fixed cadence using the selected dashboard window.'
    while True:
        # Rebuild on each tick so clients always receive a fresh snapshot from current state.
        data = build_dashboard_data(window_name, activity_span, live_update_seconds)
        payload = build_dashboard_payload(data)
        yield f'data: {json.dumps(payload)}\n\n'
        sleep_until_next_boundary(live_update_seconds, boundary_offset_seconds)
