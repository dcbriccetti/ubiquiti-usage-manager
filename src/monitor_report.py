'Console rendering helpers for usage monitor snapshots.'

from datetime import datetime

from monitor import ClientSnapshot


def print_snapshot_report(snapshots: list[ClientSnapshot], now: datetime | None = None) -> None:
    'Print one formatted monitor report for the provided client snapshots.'
    timestamp = now or datetime.now()
    print(f"\n--- Update: {timestamp.strftime('%H:%M:%S')} ---")

    header_top, header_bottom, divider, usage_col_width, speed_limit_col_width = _build_report_headers()
    print(header_top)
    print(header_bottom)
    print(divider)

    for snapshot in snapshots:
        print(
            _build_snapshot_row(
                snapshot,
                usage_col_width,
                speed_limit_col_width,
            )
        )


def _build_report_headers() -> tuple[str, str, str, int, int]:
    base_header = (
        f"{'User ID':<13} | {'Name':<20} | {'MAC':<17} | {'VLAN':<10} | {'AP':<16} | "
        f"{'Sig':<3}"
    )
    usage_col_width = 10
    speed_limit_header = "Speed Limit (kbps up/down)"
    speed_limit_col_width = len(speed_limit_header)
    usage_columns = (
        f"{'Minute':>{usage_col_width}} | {'Today':>{usage_col_width}} | "
        f"{'7 Days':>{usage_col_width}} | {'This Month':>{usage_col_width}}"
    )
    usage_group_label = " USAGE (MB) "
    header_top = (
        f"{' ' * len(base_header)} | {usage_group_label.center(len(usage_columns), '-')} | "
    )
    header_bottom = (
        f"{base_header} | {usage_columns} | {speed_limit_header:<{speed_limit_col_width}} |"
    )
    divider = "-" * len(header_bottom)
    return header_top, header_bottom, divider, usage_col_width, speed_limit_col_width


def _build_snapshot_row(
    snapshot: ClientSnapshot,
    usage_col_width: int,
    speed_limit_col_width: int,
) -> str:
    drop_suffix = " AP"
    client = snapshot.client
    ap_str = (
        client.ap_name[:-len(drop_suffix)]
        if client.ap_name.endswith(drop_suffix)
        else client.ap_name
    )
    signal_str = " " * 3 if client.signal == 0 else f"{client.signal:<3}"
    interval_str = (
        " " * usage_col_width
        if snapshot.interval_mb < 0.01
        else f"{snapshot.interval_mb:>{usage_col_width},.2f}"
    )
    total_str = (
        " " * usage_col_width
        if snapshot.day_total_mb < 0.01
        else f"{snapshot.day_total_mb:>{usage_col_width},.1f}"
    )
    last_7_days_str = (
        " " * usage_col_width
        if snapshot.last_7_days_total_mb < 0.01
        else f"{snapshot.last_7_days_total_mb:>{usage_col_width},.1f}"
    )
    calendar_month_str = (
        " " * usage_col_width
        if snapshot.calendar_month_total_mb < 0.01
        else f"{snapshot.calendar_month_total_mb:>{usage_col_width},.1f}"
    )
    speed_limit_str = (
        str(snapshot.effective_speed_limit) if snapshot.effective_speed_limit else ""
    )

    return (
        f"{client.user_id:<13} | {client.name[:20]:<20} | {client.mac:<17} | "
        f"{client.vlan_name:<10} | {ap_str:<16} | {signal_str} | {interval_str} | "
        f"{total_str} | {last_7_days_str} | {calendar_month_str} | "
        f"{speed_limit_str:<{speed_limit_col_width}} |"
    )
