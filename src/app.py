'''Flask application entry module.

This module keeps the web entrypoint intentionally small:
- creates the Flask app
- registers HTTP routes
- delegates dashboard data shaping to dashboard_service
- delegates SSE frame generation to dashboard_stream

Keeping route glue here and business/view-model logic in helper modules reduces
merge conflicts and makes testing easier because each module has a tighter scope.
'''
from datetime import date, datetime
import ipaddress
import os
import re
import time
from typing import Any, TypedDict, cast

from flask import Flask, Response, abort, jsonify, redirect, render_template, request, stream_with_context, url_for

import config as cfg
import database as db
import unifi_api as api
from database import UsageRecord
from dashboard_service import (
    build_insights_data,
    build_live_dashboard_payload,
    normalize_activity_span,
    normalize_window,
)
from dashboard_stream import event_stream
from display_format import format_voucher_data_amount, format_voucher_percent
from lan_identity import find_client_mac_for_ip, get_request_ip
from logging_config import configure_logging
from report_periods import build_report_period_context
from usage_context import (
    get_client_usage_context,
    speed_limit_option_label,
)
from wan_service import (
    build_month_usage_comparison_rows,
    build_wan_attribution_diagnostics,
    build_wan_attribution_period_rows,
    build_wan_billing_readiness,
    bytes_to_mb,
    serialize_wan_identity_rows,
    summarize_wan_by_network,
    total_wan_mb,
)


class InventoryVoucherSummary(TypedDict):
    'Grouped unactivated voucher inventory for admin display.'
    allocation_gb: int
    count: int
    total_allocation_gb: int
    oldest_generated_at: datetime
    newest_generated_at: datetime


def create_app() -> Flask:
    'Create and configure the Flask web application.'
    configure_logging()
    db.init_db()
    flask_app = Flask(__name__)
    flask_app.jinja_env.filters['voucher_data_amount'] = format_voucher_data_amount
    flask_app.jinja_env.filters['voucher_percent'] = format_voucher_percent
    live_update_seconds = 60
    live_update_boundary_offset_seconds = 3
    admin_auth_cache_seconds = 30.0
    admin_auth_cache_by_ip: dict[str, float] = {}

    def build_report_context(available_months: list[date] | None = None) -> dict[str, object]:
        'Build reusable template/query context for month-selected reports.'
        resolved_months = available_months if available_months is not None else db.get_usage_months()
        return build_report_period_context(
            request.args.get('month'),
            resolved_months,
        ).as_template_context()

    def render_radius_value(value: object) -> str:
        'Return a readable display value for optional RADIUS account fields.'
        if value is None or value == '':
            return ''
        return str(value)

    def calculate_voucher_cost_cents(allocation_gb: int) -> int:
        'Return voucher price using the configured cents-per-GB rate.'
        return int(round(allocation_gb * cfg.COST_IN_CENTS_PER_GB))

    def get_voucher_wifi_ssid() -> str:
        'Return Wi-Fi SSID display name for printed voucher instructions.'
        return str(getattr(cfg, 'PLUS_REPORT_TITLE', '') or 'Plus').strip()

    def build_voucher_radius_payload(voucher: db.PlusVoucherRecord) -> dict[str, Any]:
        'Return the local RADIUS account payload for one voucher.'
        return api.build_radius_account_payload(
            username=str(voucher.user_id),
            password=voucher.password,
        )

    def voucher_value_options() -> list[dict[str, int]]:
        'Return supported voucher values and their GB allocations.'
        return [
            {'dollars': dollars, 'allocation_gb': int((dollars * 100) / cfg.COST_IN_CENTS_PER_GB)}
            for dollars in (5, 10, 20, 50, 100)
        ]

    def summarize_inventory_vouchers(
        voucher_summaries: list[db.PlusVoucherUsageSummary],
    ) -> list[InventoryVoucherSummary]:
        'Group unactivated voucher inventory by allocation size.'
        inventory_by_allocation: dict[int, InventoryVoucherSummary] = {}
        for summary in voucher_summaries:
            if summary.activated_at is not None:
                continue
            voucher = summary.voucher
            row = inventory_by_allocation.setdefault(
                voucher.allocation_gb,
                {
                    'allocation_gb': voucher.allocation_gb,
                    'count': 0,
                    'total_allocation_gb': 0,
                    'oldest_generated_at': voucher.generated_at,
                    'newest_generated_at': voucher.generated_at,
                },
            )
            row['count'] += 1
            row['total_allocation_gb'] += voucher.allocation_gb
            row['oldest_generated_at'] = min(row['oldest_generated_at'], voucher.generated_at)
            row['newest_generated_at'] = max(row['newest_generated_at'], voucher.generated_at)

        return sorted(
            inventory_by_allocation.values(),
            key=lambda row: (row['allocation_gb'], row['count']),
            reverse=True,
        )

    def pluralize(count: int, singular: str, plural: str | None = None) -> str:
        'Return a count plus singular/plural label.'
        return f'{count} {singular if count == 1 else (plural or singular + "s")}'

    def get_client_mac_from_records(request_ip: str, client_records: list[dict[str, Any]]) -> str | None:
        'Return the client MAC whose active or recent IP matches the requester.'
        for client in client_records:
            client_ip = client.get('ip') or client.get('last_ip')
            client_mac = client.get('mac')
            if isinstance(client_ip, str) and client_ip == request_ip and isinstance(client_mac, str):
                return client_mac.lower()
        return None

    def get_live_client_record_by_mac(
        mac: str,
        live_clients: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any] | None:
        'Return live UniFi station payload for one MAC, if currently connected.'
        target_mac = mac.lower()
        for client in live_clients if live_clients is not None else api.get_api_data("stat/sta"):
            raw_mac = client.get("mac")
            if isinstance(raw_mac, str) and raw_mac.lower() == target_mac:
                return client
        return None

    def is_plus_network(vlan_name: str | None) -> bool:
        'Return True when the VLAN/network label represents the Plus network.'
        if not vlan_name:
            return False
        plus_network_names = {
            str(network_name).strip().lower()
            for network_name in getattr(cfg, 'PLUS_NETWORK_NAMES', {'Plus'})
            if str(network_name).strip()
        }
        if plus_report_title := str(getattr(cfg, 'PLUS_REPORT_TITLE', '')).strip():
            plus_network_names.add(plus_report_title.lower())
        return vlan_name.strip().lower() in plus_network_names

    def is_plus_admin_user(user_id: str | None, vlan_name: str | None) -> bool:
        'Return True when requester is a Plus user whose RADIUS username is in admin allowlist.'
        if not is_plus_network(vlan_name) or not user_id:
            return False
        plus_admins = {
            str(admin_user_id).strip().lower()
            for admin_user_id in getattr(cfg, 'PLUS_ADMINS', set())
            if str(admin_user_id).strip()
        }
        return user_id.strip().lower() in plus_admins

    def get_live_client_admin_status(
        detected_mac: str,
        live_clients: list[dict[str, Any]] | None = None,
    ) -> bool | None:
        'Return live admin status for a connected client, or None when identity is incomplete.'
        live_client = get_live_client_record_by_mac(detected_mac, live_clients)
        if live_client is None:
            return None

        live_vlan_name = live_client.get('network')
        if not isinstance(live_vlan_name, str):
            return None

        if not is_plus_network(live_vlan_name):
            return False

        live_user_id = (
            live_client.get('1x_identity')
            or live_client.get('identity')
            or live_client.get('last_1x_identity')
        )
        if not live_user_id:
            last_identities = live_client.get('last_1x_identities')
            if isinstance(last_identities, list) and last_identities:
                first_identity = last_identities[0]
                live_user_id = first_identity if isinstance(first_identity, str) else None

        if not isinstance(live_user_id, str) or not live_user_id.strip():
            return None

        return is_plus_admin_user(live_user_id, live_vlan_name)

    def request_ip_is_plus_admin(request_ip: str) -> bool:
        'Return True when request IP matches configured admin IP/CIDR allowlist.'
        try:
            requester_ip = ipaddress.ip_address(request_ip)
        except ValueError:
            flask_app.logger.warning('Ignoring invalid requester IP for admin allowlist: %s', request_ip)
            return False

        for admin_ip_entry in getattr(cfg, 'PLUS_ADMIN_IPS', set()):
            admin_ip_text = str(admin_ip_entry).strip()
            if not admin_ip_text:
                continue
            try:
                if '/' in admin_ip_text:
                    if requester_ip in ipaddress.ip_network(admin_ip_text, strict=False):
                        return True
                elif requester_ip == ipaddress.ip_address(admin_ip_text):
                    return True
            except ValueError:
                flask_app.logger.warning('Ignoring invalid PLUS_ADMIN_IPS entry: %s', admin_ip_text)

        return False

    def resolve_request_ip() -> str | None:
        'Return request IP, allowing DEV_REQUEST_IP override for local/remote testing.'
        if dev_request_ip := os.getenv("DEV_REQUEST_IP", "").strip():
            return dev_request_ip
        return get_request_ip(request)

    def resolve_my_usage_mac(request_ip: str | None) -> tuple[str | None, str]:
        'Resolve MAC for my-usage routes, allowing MY_USAGE_OVERRIDE_MAC env override.'
        override_mac_raw = os.getenv("MY_USAGE_OVERRIDE_MAC", "").strip()
        if override_mac_raw:
            override_mac_normalized = override_mac_raw.lower().replace('-', ':')
            if re.fullmatch(r'(?:[0-9a-f]{2}:){5}[0-9a-f]{2}', override_mac_normalized):
                return override_mac_normalized, ''
            return None, "MY_USAGE_OVERRIDE_MAC is set but is not a valid MAC address."

        if not request_ip:
            return None, "Could not determine your client IP address from this request."

        if detected_mac := find_client_mac_for_ip(request_ip):
            return detected_mac, ''

        return None, (
            "Could not map your IP to a UniFi client right now. "
            "Try again in a moment after generating some network activity."
        )

    def warn_missing_radius_identity(record: UsageRecord, request_ip: str | None, detected_mac: str | None) -> None:
        'Log warning when Plus-network client metadata is missing RADIUS user_id.'
        if is_plus_network(record.vlan) and not (record.user_id and record.user_id.strip()):
            flask_app.logger.warning(
                (
                    'Plus-network client missing RADIUS user_id '
                    '(request_ip=%s detected_mac=%s record_mac=%s vlan=%s name=%s ap_name=%s)'
                ),
                request_ip or '',
                detected_mac or '',
                record.mac,
                record.vlan or '',
                record.name or '',
                record.ap_name or '',
            )

    def requester_is_plus_admin() -> bool:
        'Resolve current requester and return whether they are a Plus admin.'
        request_ip = resolve_request_ip()
        if not request_ip:
            return False
        request_ip_text: str = request_ip

        now_monotonic = time.monotonic()
        if admin_auth_cache_by_ip.get(request_ip_text, 0.0) > now_monotonic:
            return True

        def remember_admin(result: bool) -> bool:
            if result:
                admin_auth_cache_by_ip[request_ip_text] = time.monotonic() + admin_auth_cache_seconds
            return result

        if request_ip_is_plus_admin(request_ip_text):
            return remember_admin(True)

        live_clients = api.get_api_data("stat/sta")
        detected_mac = get_client_mac_from_records(request_ip_text, live_clients) or find_client_mac_for_ip(request_ip_text)
        if not detected_mac:
            return False

        if (live_admin_status := get_live_client_admin_status(detected_mac, live_clients)) is not None:
            return remember_admin(live_admin_status)

        usage_history = db.get_usage_history(detected_mac, limit=1)
        if usage_history:
            latest_record = usage_history[0]
            warn_missing_radius_identity(latest_record, request_ip_text, detected_mac)
            return remember_admin(is_plus_admin_user(latest_record.user_id, latest_record.vlan))

        return False
    @flask_app.route("/")
    def dashboard():
        'Render the dashboard with live snapshots and daily usage summaries.'
        if not requester_is_plus_admin():
            return redirect(url_for("my_usage"))

        window_name = normalize_window(request.args.get("window"))
        activity_span = normalize_activity_span(request.args.get("activity_span"))
        dashboard_data = build_live_dashboard_payload(window_name, activity_span, live_update_seconds)
        return render_template(
            "dashboard.html",
            initial_dashboard_payload=dashboard_data,
        )

    @flask_app.route("/insights")
    def insights():
        'Render deeper month analytics panels.'
        if not requester_is_plus_admin():
            abort(403)

        period_context = build_report_context(db.get_usage_months())
        period_start = cast(datetime, period_context['period_start'])
        period_end = cast(datetime, period_context['period_end'])
        selected_month = cast(date, period_context['selected_month'])
        current_month = date(datetime.now().year, datetime.now().month, 1)
        insights_data = build_insights_data(
            period_start=period_start,
            period_end=period_end,
            current_month_label=selected_month.strftime('%b'),
            report_period_label=str(period_context['report_period_label']),
            include_live_organization_paid_clients=selected_month == current_month,
        )
        return render_template(
            "insights.html",
            selected_month=period_context['selected_month'],
            selected_month_value=period_context['selected_month_value'],
            current_month_value=period_context['current_month_value'],
            previous_month_value=period_context['previous_month_value'],
            month_options=period_context['month_options'],
            **insights_data,
        )

    @flask_app.route("/wan")
    def wan_usage():
        'Render WAN flow usage imported from nfdump/IPFIX captures.'
        if not requester_is_plus_admin():
            abort(403)

        now = datetime.now()
        today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        month_start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        today_rows = db.get_wan_usage_by_identity(period_start=today_start, period_end=now)
        month_rows = db.get_wan_usage_by_identity(period_start=month_start, period_end=now)
        decorated_today_rows = serialize_wan_identity_rows(today_rows)
        decorated_month_rows = serialize_wan_identity_rows(month_rows)
        first_wan_flow_at = db.get_first_wan_flow_time()
        month_comparison_start = max(month_start, first_wan_flow_at) if first_wan_flow_at else month_start
        recent_imports = db.get_recent_flow_imports(limit=12)
        latest_import = recent_imports[0] if recent_imports else None
        latest_import_age_minutes = (
            int((now - latest_import.imported_at).total_seconds() // 60)
            if latest_import
            else None
        )
        client_display_threshold_mb = 0.01
        visible_today_rows = [
            row
            for row in decorated_today_rows
            if total_wan_mb(row) >= client_display_threshold_mb
        ]
        hidden_tiny_client_count = len(decorated_today_rows) - len(visible_today_rows)

        total_today_download_bytes = sum(row.download_bytes for row in today_rows)
        total_today_upload_bytes = sum(row.upload_bytes for row in today_rows)
        total_month_download_bytes = sum(row.download_bytes for row in month_rows)
        total_month_upload_bytes = sum(row.upload_bytes for row in month_rows)
        total_today_wan_mb = bytes_to_mb(total_today_download_bytes + total_today_upload_bytes)
        total_month_wan_mb = bytes_to_mb(total_month_download_bytes + total_month_upload_bytes)
        total_today_unifi_mb = db.get_total_today_usage()
        total_month_unifi_mb = db.get_total_calendar_month_usage()
        reconciliation_rows = [
            {
                'label': 'Today',
                'unifi_mb': total_today_unifi_mb,
                'wan_mb': total_today_wan_mb,
                'difference_mb': total_today_wan_mb - total_today_unifi_mb,
                'wan_pct_of_unifi': (total_today_wan_mb / total_today_unifi_mb * 100.0)
                if total_today_unifi_mb
                else 0.0,
            },
            {
                'label': now.strftime('%b'),
                'unifi_mb': total_month_unifi_mb,
                'wan_mb': total_month_wan_mb,
                'difference_mb': total_month_wan_mb - total_month_unifi_mb,
                'wan_pct_of_unifi': (total_month_wan_mb / total_month_unifi_mb * 100.0)
                if total_month_unifi_mb
                else 0.0,
            },
        ]
        today_attribution_diagnostics = build_wan_attribution_diagnostics(decorated_today_rows)
        month_attribution_diagnostics = build_wan_attribution_diagnostics(decorated_month_rows)

        return render_template(
            "wan_usage.html",
            generated_at=now,
            today_rows=visible_today_rows,
            month_rows=decorated_month_rows,
            month_usage_comparison_rows=build_month_usage_comparison_rows(
                decorated_month_rows,
                period_start=month_comparison_start,
                period_end=now,
            ),
            month_comparison_start=month_comparison_start,
            month_comparison_end=now,
            today_attribution_diagnostics=today_attribution_diagnostics,
            month_attribution_diagnostics=month_attribution_diagnostics,
            attribution_period_rows=build_wan_attribution_period_rows(
                today_attribution_diagnostics,
                month_attribution_diagnostics,
            ),
            wan_billing_readiness=build_wan_billing_readiness(
                month_attribution_diagnostics,
                latest_import_age_minutes,
            ),
            today_network_rows=summarize_wan_by_network(decorated_today_rows),
            month_network_rows=summarize_wan_by_network(decorated_month_rows),
            recent_imports=recent_imports,
            latest_import=latest_import,
            latest_import_age_minutes=latest_import_age_minutes,
            internal_networks=sorted(str(network) for network in getattr(cfg, 'INTERNAL_NETWORKS', set())),
            client_display_threshold_mb=client_display_threshold_mb,
            hidden_tiny_client_count=hidden_tiny_client_count,
            reconciliation_rows=reconciliation_rows,
            bytes_to_mb=bytes_to_mb,
            total_today_download_mb=bytes_to_mb(total_today_download_bytes),
            total_today_upload_mb=bytes_to_mb(total_today_upload_bytes),
            total_month_download_mb=bytes_to_mb(total_month_download_bytes),
            total_month_upload_mb=bytes_to_mb(total_month_upload_bytes),
        )

    @flask_app.route("/radius/users")
    def radius_users():
        'Render local RADIUS users configured in UniFi.'
        if not requester_is_plus_admin():
            abort(403)

        accounts = api.get_radius_accounts()
        radius_user_rows = [
            {
                'id': render_radius_value(account.get('_id')),
                'name': render_radius_value(account.get('name')),
                'vlan': render_radius_value(account.get('vlan') or account.get('network_id')),
                'tunnel_type': render_radius_value(account.get('tunnel_type')),
                'tunnel_medium_type': render_radius_value(account.get('tunnel_medium_type')),
            }
            for account in accounts
        ]
        radius_user_rows.sort(key=lambda row: row['name'].lower())

        return render_template(
            "radius_users.html",
            generated_at=datetime.now(),
            radius_user_rows=radius_user_rows,
        )

    @flask_app.route("/vouchers", methods=["GET", "POST"])
    def plus_vouchers():
        'Generate and list home-grown Plus login vouchers.'
        if not requester_is_plus_admin():
            abort(403)

        generated_vouchers: list[db.PlusVoucherRecord] = []
        voucher_form_message = ''
        selected_count = '10'
        selected_value_dollars = '5'

        if request.method == "POST":
            selected_count = request.form.get("count", selected_count).strip()
            selected_value_dollars = request.form.get("value_dollars", selected_value_dollars).strip()
            try:
                count = int(selected_count)
                value_dollars = int(selected_value_dollars)
                allowed_values = {option['dollars'] for option in voucher_value_options()}
                if value_dollars not in allowed_values:
                    raise ValueError('Please select a valid voucher value.')
                allocation_gb = int((value_dollars * 100) / cfg.COST_IN_CENTS_PER_GB)
                if count > 100:
                    raise ValueError('Generate at most 100 vouchers at a time.')
                generated_vouchers = db.create_plus_vouchers(count, allocation_gb)
                radius_failures: list[str] = []
                for voucher in generated_vouchers:
                    payload = build_voucher_radius_payload(voucher)
                    created, error_message = api.create_radius_account(payload)
                    if not created:
                        radius_failures.append(f'{voucher.user_id}: {error_message}')
                if radius_failures:
                    voucher_form_message = (
                        f'Generated {pluralize(len(generated_vouchers), "voucher")}, but '
                        f'{pluralize(len(radius_failures), "RADIUS account")} failed: '
                        f'{", ".join(radius_failures)}'
                    )
                else:
                    generated_count = len(generated_vouchers)
                    voucher_form_message = (
                        f'Generated {pluralize(generated_count, "voucher")} and '
                        f'created {pluralize(generated_count, "UniFi RADIUS account")}.'
                    )
            except ValueError as exc:
                voucher_form_message = str(exc)

        voucher_rows = db.get_plus_vouchers()
        active_voucher_summaries = db.get_active_plus_voucher_summaries()
        return render_template(
            "plus_vouchers.html",
            generated_vouchers=generated_vouchers,
            voucher_rows=voucher_rows,
            voucher_form_message=voucher_form_message,
            selected_count=selected_count,
            selected_value_dollars=selected_value_dollars,
            voucher_value_options=voucher_value_options(),
            unconsumed_voucher_count=db.get_unconsumed_plus_voucher_count(),
            active_voucher_summaries=[
                summary for summary in active_voucher_summaries if summary.activated_at is not None
            ],
            inventory_voucher_summaries=summarize_inventory_vouchers(active_voucher_summaries),
            voucher_cost_cents=calculate_voucher_cost_cents,
        )

    @flask_app.route("/vouchers/batches/<batch_id>/print")
    def plus_voucher_batch_print(batch_id: str):
        'Render a print-optimized sheet for one voucher batch.'
        if not requester_is_plus_admin():
            abort(403)

        vouchers = db.get_plus_voucher_batch(batch_id)
        if not vouchers:
            abort(404)

        return render_template(
            "plus_voucher_print.html",
            batch_id=batch_id,
            vouchers=vouchers,
            voucher_cost_cents=calculate_voucher_cost_cents,
            voucher_wifi_ssid=get_voucher_wifi_ssid(),
            generated_at=datetime.now(),
        )

    @flask_app.route("/api/dashboard-snapshot")
    def dashboard_snapshot():
        'Return dashboard snapshot data for incremental in-page refresh.'
        if not requester_is_plus_admin():
            abort(403)

        window_name = normalize_window(request.args.get("window"))
        activity_span = normalize_activity_span(request.args.get("activity_span"))
        return jsonify(build_live_dashboard_payload(window_name, activity_span, live_update_seconds))

    @flask_app.route("/api/dashboard-stream")
    def dashboard_stream():
        'Stream dashboard updates over Server-Sent Events.'
        if not requester_is_plus_admin():
            abort(403)

        window_name = normalize_window(request.args.get("window"))
        activity_span = normalize_activity_span(request.args.get("activity_span"))
        response = Response(
            stream_with_context(
                event_stream(
                    window_name,
                    activity_span,
                    live_update_seconds,
                    live_update_boundary_offset_seconds,
                )
            ),
            mimetype="text/event-stream",
        )
        response.headers["Cache-Control"] = "no-cache"
        response.headers["X-Accel-Buffering"] = "no"
        return response

    @flask_app.route("/clients/<mac>")
    def client_detail(mac: str):
        'Render detail view for one client MAC address.'
        if not requester_is_plus_admin():
            abort(403)

        try:
            context = get_client_usage_context(mac)
            return render_template(
                "usage_detail.html",
                page_title=f"{context['latest_record'].name or context['mac']} | UniFi Usage",
                can_set_speed_limit=False,
                speed_limit_options=[],
                selected_speed_limit_name="",
                speed_limit_form_message="",
                **context,
            )
        except LookupError:
            abort(404)

    @flask_app.route("/clients/<mac>/usage-today-embed")
    def client_usage_today_embed(mac: str):
        'Render embeddable "Usage Today" panel for one client MAC.'
        if not requester_is_plus_admin():
            abort(403)

        try:
            context = get_client_usage_context(mac)
        except LookupError:
            abort(404)

        daily_scale = next((scale for scale in context['usage_scales'] if scale['key'] == 'daily'), None)
        if daily_scale is None:
            abort(404)

        return render_template(
            "client_usage_today_embed.html",
            mac=context['mac'],
            usage_scale=daily_scale,
        )

    @flask_app.route("/my-usage", methods=["GET", "POST"])
    def my_usage():
        'Render usage details for the LAN client identified by request IP/MAC mapping.'
        request_ip = resolve_request_ip()
        detected_mac, lookup_error = resolve_my_usage_mac(request_ip)
        if lookup_error:
            return render_template(
                "usage_detail.html",
                page_title="My Usage | UniFi Usage",
                error_message=lookup_error,
                request_ip=request_ip or "",
                detected_mac="",
            )

        if detected_mac is None:
            return render_template(
                "usage_detail.html",
                page_title="My Usage | UniFi Usage",
                error_message="Could not determine your device MAC address from this request.",
                request_ip=request_ip,
                detected_mac="",
            )

        try:
            context = get_client_usage_context(detected_mac)
        except LookupError:
            return render_template(
                "usage_detail.html",
                page_title="My Usage | UniFi Usage",
                error_message="We identified your device, but no usage record is available yet.",
                request_ip=request_ip,
                detected_mac=detected_mac,
            )

        warn_missing_radius_identity(context['latest_record'], request_ip, detected_mac)
        plus_user = is_plus_network(context['latest_record'].vlan)
        can_set_speed_limit = plus_user and cfg.SELF_SERVICE_SPEED_LIMIT_ENABLED
        speed_limits = api.get_speed_limits() if can_set_speed_limit else []
        selected_speed_limit_name = context['latest_record'].profile or ''
        speed_limit_form_message = ''

        if request.method == "POST":
            if not can_set_speed_limit:
                speed_limit_form_message = 'Speed-limit changes are temporarily unavailable.'
            else:
                requested_limit_name = request.form.get("speed_limit_name", "").strip()
                speed_limits_by_name = {limit.name: limit for limit in speed_limits}
                selected_limit = speed_limits_by_name.get(requested_limit_name)

                if not selected_limit:
                    speed_limit_form_message = 'Please select a valid speed limit.'
                elif not (live_client := get_live_client_record_by_mac(detected_mac)):
                    speed_limit_form_message = 'Your device must be online to apply a speed-limit change.'
                else:
                    unifi_client_id = live_client.get('_id')
                    if not isinstance(unifi_client_id, str) or not unifi_client_id:
                        speed_limit_form_message = 'Could not identify this device in UniFi right now.'
                    elif api.set_user_group(unifi_client_id, selected_limit.id):
                        selected_speed_limit_name = selected_limit.name
                        context['latest_record'].profile = selected_limit.name
                        speed_limit_form_message = f'Speed limit updated to {speed_limit_option_label(selected_limit)}.'
                    else:
                        speed_limit_form_message = 'Could not apply speed limit. Please try again.'

        speed_limit_options = [
            {'name': limit.name, 'label': speed_limit_option_label(limit)}
            for limit in speed_limits
        ]

        return render_template(
            "usage_detail.html",
            page_title="My Usage | UniFi Usage",
            request_ip=request_ip,
            detected_mac=detected_mac,
            can_set_speed_limit=can_set_speed_limit,
            speed_limit_options=speed_limit_options,
            selected_speed_limit_name=selected_speed_limit_name,
            speed_limit_form_message=speed_limit_form_message,
            **context,
        )

    return flask_app


app = create_app()


if __name__ == "__main__":
    flask_debug = os.getenv("FLASK_DEBUG", "").strip().lower() in {"1", "true", "yes", "on"}
    app.run(debug=flask_debug, use_reloader=flask_debug, host="127.0.0.1", port=5051)
