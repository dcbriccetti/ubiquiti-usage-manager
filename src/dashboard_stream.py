'''Dashboard Server-Sent Events (SSE) stream helpers.

This module provides the streaming loop used by `/api/dashboard-stream`.
It rebuilds dashboard data at a fixed interval and emits JSON payloads as
SSE frames (`data: ...`) so connected browsers can update in near real time.
'''

import json
import time
from collections.abc import Iterator

import database as db
from dashboard_events import current_dashboard_data_version, wait_for_dashboard_data_change
from dashboard_service import ActivitySpan, WindowName, build_live_dashboard_payload, clear_dashboard_wan_cache


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


def next_boundary_delay_seconds(interval_seconds: int, offset_seconds: int = 0) -> float:
    'Return seconds until the next wall-clock interval boundary plus optional offset.'
    if interval_seconds <= 0:
        return 0.0

    now = time.time()
    next_boundary = (int(now // interval_seconds) + 1) * interval_seconds
    next_tick = next_boundary + max(0, offset_seconds)
    if next_tick <= now:
        next_tick += interval_seconds
    return max(0.0, next_tick - now)


def latest_flow_import_marker() -> tuple[str, str] | None:
    'Return a compact marker for the latest imported capture file.'
    recent_imports = db.get_recent_flow_imports(limit=1)
    if not recent_imports:
        return None
    latest_import = recent_imports[0]
    return latest_import.source_file, latest_import.imported_at.isoformat()


def event_stream(
    window_name: WindowName,
    activity_span: ActivitySpan,
    live_update_seconds: int,
    boundary_offset_seconds: int = 0,
) -> Iterator[str]:
    'Yield SSE frames on a fixed cadence and shortly after flow imports.'
    last_event_version = current_dashboard_data_version()
    last_import_marker = latest_flow_import_marker()
    while True:
        # Rebuild on each tick so clients always receive a fresh snapshot from current state.
        payload = build_live_dashboard_payload(window_name, activity_span, live_update_seconds)
        yield f'data: {json.dumps(payload)}\n\n'
        deadline = time.monotonic() + next_boundary_delay_seconds(live_update_seconds, boundary_offset_seconds)
        while True:
            timeout_seconds = min(1.0, max(0.0, deadline - time.monotonic()))
            next_event_version = wait_for_dashboard_data_change(last_event_version, timeout_seconds)
            if next_event_version != last_event_version:
                last_event_version = next_event_version
                clear_dashboard_wan_cache()
                break

            next_import_marker = latest_flow_import_marker()
            if next_import_marker != last_import_marker:
                last_import_marker = next_import_marker
                clear_dashboard_wan_cache()
                break

            if time.monotonic() >= deadline:
                break
