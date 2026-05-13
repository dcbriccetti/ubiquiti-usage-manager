'''Database access layer for usage metering and dashboard aggregation.

This module owns SQLite/SQLAlchemy setup and all persisted usage queries so
monitoring/runtime code does not need direct SQL concerns.
'''

import sqlite3
import logging
from bisect import bisect_right
from dataclasses import dataclass
from datetime import datetime, date, time, timedelta
from pathlib import Path
from typing import Any, Optional, cast
from sqlalchemy import UniqueConstraint, case, create_engine, String, func, or_, select, event
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker

from clientinfo import ClientInfo

# --- SETUP & CONCURRENCY ---
DB_PATH = Path(__file__).resolve().parent.parent / "meter.db"
DB_URL = f"sqlite:///{DB_PATH}"
engine = create_engine(DB_URL, echo=False)
logger = logging.getLogger(__name__)

@event.listens_for(engine, "connect")
def set_sqlite_pragma(dbapi_connection: sqlite3.Connection, _connection_record: object) -> None:
    'Apply SQLite pragmas for better concurrent read/write behavior.'
    cursor = dbapi_connection.cursor()
    try:
        # If another process has the DB locked momentarily, continue with defaults.
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.execute("PRAGMA synchronous=NORMAL")
    except sqlite3.OperationalError as exc:
        logger.warning("SQLite PRAGMA setup skipped: %s", exc)
    finally:
        cursor.close()

SessionLocal = sessionmaker(bind=engine)


PERFORMANCE_INDEX_SPECS = [
    ("ix_wan_flow_usage_started_client", "wan_flow_usage", ("started_at", "client_ip")),
    ("ix_wan_flow_usage_client_started", "wan_flow_usage", ("client_ip", "started_at")),
    ("ix_client_ip_identities_ip_observed", "client_ip_identities", ("ip_address", "observed_at")),
    ("ix_client_ip_identities_mac_observed", "client_ip_identities", ("mac", "observed_at")),
    ("ix_client_ip_identities_user_observed", "client_ip_identities", ("user_id", "observed_at")),
    ("ix_usage_records_mac_timestamp", "usage_records", ("mac", "timestamp")),
    ("ix_plus_vouchers_user_active", "plus_vouchers", ("user_id", "consumed_at", "generated_at")),
]


def ensure_performance_indexes() -> None:
    'Create composite indexes used by the higher-volume reporting queries.'
    preparer = engine.dialect.identifier_preparer
    with engine.begin() as connection:
        for index_name, table_name, column_names in PERFORMANCE_INDEX_SPECS:
            table = Base.metadata.tables.get(table_name)
            if table is None:
                logger.warning("Skipping index %s because table %s is not registered.", index_name, table_name)
                continue
            quoted_columns = ', '.join(preparer.quote(column_name) for column_name in column_names)
            statement = (
                f"CREATE INDEX IF NOT EXISTS {preparer.quote(index_name)} "
                f"ON {preparer.quote(table.name)} ({quoted_columns})"
            )
            connection.exec_driver_sql(statement)


def _month_period_bounds(
    period_start: datetime | None = None,
    period_end: datetime | None = None,
) -> tuple[datetime, datetime, date, date]:
    'Return datetime/date bounds for current month or an explicit period.'
    now = datetime.now()
    start_dt = period_start or datetime.combine(date(now.year, now.month, 1), time.min)
    end_dt = period_end or now
    return start_dt, end_dt, start_dt.date(), end_dt.date()


class Base(DeclarativeBase):
    'Base class for SQLAlchemy ORM models.'
    pass


@dataclass(frozen=True, kw_only=True)
class DailyUsageSummary:
    'Aggregated daily usage metrics for one client MAC.'
    mac: str
    user_id: str | None
    name: str | None
    vlan: str | None
    profile: str | None
    ap_name: str | None
    signal: int | None
    total_mb: float
    last_seen: datetime
    usage_entries: int


@dataclass(frozen=True, kw_only=True)
class UsageWindowSummary:
    'Per-client usage rollup and latest-known metadata for dashboard windows.'
    mac: str
    user_id: str | None
    name: str | None
    vlan: str | None
    profile: str | None
    ap_name: str | None
    day_total_mb: float
    last_7_days_total_mb: float
    calendar_month_total_mb: float
    last_seen: datetime


@dataclass(frozen=True, kw_only=True)
class GlobalTopUser:
    'Global monthly leaderboard row for one user/client.'
    mac: str
    name: str
    user_id: str
    vlan_name: str = ''
    total_mb: float
    active_minutes: int


@dataclass(frozen=True, kw_only=True)
class GlobalTopAccessPoint:
    'Global monthly hotspot row for one access point.'
    ap_name: str
    total_mb: float
    active_minutes: int


@dataclass(frozen=True, kw_only=True)
class GlobalInsights:
    'Global month-to-date dashboard analytics.'
    active_users_min: int
    active_users_mean: float
    active_users_max: int
    active_users_today: int
    days_in_period: int
    active_users_daily_x_labels: list[int]
    active_users_daily_full_labels: list[str]
    active_users_daily_counts: list[int]
    top_users: list[GlobalTopUser]
    top_access_points: list[GlobalTopAccessPoint]


@dataclass(frozen=True, kw_only=True)
class GlobalDailyNetworkUsage:
    'Daily month-to-date totals split by Basic/Plus networks.'
    usage_day: date
    basic_mb: float
    plus_mb: float
    basic_minutes: int
    plus_minutes: int


@dataclass(frozen=True, kw_only=True)
class GlobalWanHourlyUsage:
    'Hourly month-to-date WAN usage total.'
    bucket_start: datetime
    total_mb: float


@dataclass(frozen=True, kw_only=True)
class GlobalDailyWanVlanUsage:
    'Daily WAN usage totals for one VLAN.'
    vlan: str
    daily_mb: list[float]


@dataclass
class DailyNetworkUsageBucket:
    'Mutable accumulator for one day of Basic/Plus usage.'
    basic_mb: float = 0.0
    plus_mb: float = 0.0
    basic_minutes: int = 0
    plus_minutes: int = 0


@dataclass(frozen=True, kw_only=True)
class GlobalPayerSplit:
    'Month-to-date totals split by payer classification.'
    organization_paid_total_mb: float
    organization_paid_minutes: int
    user_paid_total_mb: float
    user_paid_minutes: int


@dataclass(frozen=True, kw_only=True)
class GlobalConcurrencyInsights:
    'Month-to-date concurrency analytics for peak and heatmap visuals.'
    daily_x_labels: list[int]
    daily_full_labels: list[str]
    daily_peak_counts: list[int]
    daily_peak_time_labels: list[str]
    heatmap_day_labels: list[str]
    heatmap_hour_labels: list[str]
    heatmap_values: list[list[float]]
    heatmap_sample_counts: list[list[int]]


@dataclass(frozen=True, kw_only=True)
class ThrottlingChangeImpact:
    'Before/after usage metrics around one profile-change event.'
    changed_at: datetime
    mac: str
    user_id: str
    name: str
    from_profile: str
    to_profile: str
    before_avg_mb_per_day: float
    after_avg_mb_per_day: float
    before_throttled_pct: float
    after_throttled_pct: float
    before_active_minutes: int
    after_active_minutes: int


@dataclass(frozen=True, kw_only=True)
class GlobalThrottlingEffectiveness:
    'Month-to-date throttling effectiveness aggregates and profile-change deltas.'
    profile_minutes: dict[str, int]
    total_active_minutes: int
    throttled_minutes: int
    throttled_pct: float
    change_events: list[ThrottlingChangeImpact]


@dataclass(frozen=True, kw_only=True)
class PlusVoucherRecord:
    'Generated Plus voucher credentials and usage allocation.'
    id: int
    batch_id: str
    user_id: int
    password: str
    allocation_gb: int
    generated_at: datetime
    consumed_at: datetime | None


@dataclass(frozen=True, kw_only=True)
class PlusVoucherUsageSummary:
    'Admin-facing active voucher balance summary.'
    voucher: PlusVoucherRecord
    activated_at: datetime | None
    used_mb: float
    remaining_mb: float
    used_pct: float


@dataclass(frozen=True, kw_only=True)
class WanFlowUsageRecord:
    'One WAN-classified flow row imported from nfdump.'
    started_at: datetime
    ended_at: datetime
    duration_seconds: float
    proto: str
    src_ip: str
    src_port: int | None
    dst_ip: str
    dst_port: int | None
    packets: int
    bytes: int
    direction: str
    client_ip: str
    source_file: str


@dataclass(frozen=True, kw_only=True)
class WanClientUsageSummary:
    'WAN upload/download rollup for one internal client IP.'
    client_ip: str
    upload_bytes: int
    download_bytes: int
    flow_count: int


@dataclass(frozen=True, kw_only=True)
class WanIdentityUsageSummary:
    'WAN upload/download rollup attributed to client identity at flow time.'
    client_ip: str
    mac: str
    name: str
    user_id: str
    vlan: str
    upload_bytes: int
    download_bytes: int
    flow_count: int


@dataclass(frozen=True, kw_only=True)
class WanMacFlowUsage:
    'One WAN flow attributed to a specific client MAC.'
    source_file: str
    started_at: datetime
    bytes: int
    direction: str


@dataclass(frozen=True, kw_only=True)
class FlowImportRecord:
    'Import bookkeeping view-model for one nfcapd capture file.'
    source_file: str
    imported_at: datetime
    record_count: int
    skipped_count: int


@dataclass(frozen=True, kw_only=True)
class ClientIpIdentityRecord:
    'Latest-known UniFi identity observed for one client IP address.'
    observed_at: datetime
    ip_address: str
    mac: str
    name: str
    user_id: str
    vlan: str

# --- MODELS ---
class UsageRecord(Base):
    'Ledger row storing one non-zero usage interval.'
    __tablename__ = "usage_records"

    id:        Mapped[int]           = mapped_column(primary_key=True, autoincrement=True)
    timestamp: Mapped[datetime]      = mapped_column(default=datetime.now, index=True)
    mac:       Mapped[str]           = mapped_column(String(17), index=True)
    user_id:   Mapped[Optional[str]] = mapped_column(String(30))
    name:      Mapped[Optional[str]] = mapped_column(String(50))
    vlan:      Mapped[Optional[str]] = mapped_column(String(20))
    mb_used:   Mapped[float]         = mapped_column()
    profile:   Mapped[Optional[str]] = mapped_column(String(30))
    ap_name:   Mapped[Optional[str]] = mapped_column(String(50))
    signal:    Mapped[Optional[int]] = mapped_column()


class PlusVoucher(Base):
    'One generated paper voucher for Plus network access.'
    __tablename__ = "plus_vouchers"

    id:            Mapped[int]                = mapped_column(primary_key=True, autoincrement=True)
    batch_id:      Mapped[str]                = mapped_column(String(32), index=True)
    user_id:       Mapped[int]                = mapped_column(index=True)
    password:      Mapped[str]                = mapped_column(String(24))
    allocation_gb: Mapped[int]                = mapped_column()
    generated_at:  Mapped[datetime]           = mapped_column(default=datetime.now, index=True)
    consumed_at:   Mapped[Optional[datetime]] = mapped_column(index=True)


class FlowImport(Base):
    'Bookkeeping row for one completed nfcapd file imported into SQLite.'
    __tablename__ = "flow_imports"

    id:            Mapped[int]      = mapped_column(primary_key=True, autoincrement=True)
    source_file:   Mapped[str]      = mapped_column(String(255), unique=True, index=True)
    imported_at:   Mapped[datetime] = mapped_column(default=datetime.now, index=True)
    record_count:  Mapped[int]      = mapped_column()
    skipped_count: Mapped[int]      = mapped_column(default=0)


class WanFlowUsage(Base):
    'WAN-classified flow imported from nfdump/IPFIX captures.'
    __tablename__ = "wan_flow_usage"
    __table_args__ = (
        UniqueConstraint(
            "source_file",
            "started_at",
            "proto",
            "src_ip",
            "src_port",
            "dst_ip",
            "dst_port",
            "packets",
            "bytes",
            name="uq_wan_flow_usage_source_tuple",
        ),
    )

    id:               Mapped[int]      = mapped_column(primary_key=True, autoincrement=True)
    source_file:      Mapped[str]      = mapped_column(String(255), index=True)
    started_at:       Mapped[datetime] = mapped_column(index=True)
    ended_at:         Mapped[datetime] = mapped_column(index=True)
    duration_seconds: Mapped[float]    = mapped_column()
    proto:            Mapped[str]      = mapped_column(String(12), index=True)
    src_ip:           Mapped[str]      = mapped_column(String(45), index=True)
    src_port:         Mapped[Optional[int]] = mapped_column(index=True)
    dst_ip:           Mapped[str]      = mapped_column(String(45), index=True)
    dst_port:         Mapped[Optional[int]] = mapped_column(index=True)
    packets:          Mapped[int]      = mapped_column()
    bytes:            Mapped[int]      = mapped_column()
    direction:        Mapped[str]      = mapped_column(String(8), index=True)
    client_ip:        Mapped[str]      = mapped_column(String(45), index=True)


class ClientIpIdentity(Base):
    'Observed mapping from client IP address to UniFi identity metadata.'
    __tablename__ = "client_ip_identities"

    id:          Mapped[int]      = mapped_column(primary_key=True, autoincrement=True)
    observed_at: Mapped[datetime] = mapped_column(default=datetime.now, index=True)
    ip_address:  Mapped[str]      = mapped_column(String(45), index=True)
    mac:         Mapped[str]      = mapped_column(String(17), index=True)
    name:        Mapped[str]      = mapped_column(String(80))
    user_id:     Mapped[str]      = mapped_column(String(80))
    vlan:        Mapped[str]      = mapped_column(String(40), index=True)


# --- DATABASE API ---
def init_db() -> None:
    'Create database tables if they do not already exist.'
    Base.metadata.create_all(bind=engine)
    ensure_performance_indexes()


def flow_import_exists(source_file: str) -> bool:
    'Return True when an nfcapd source file has already been imported.'
    stmt = select(func.count()).select_from(FlowImport).where(FlowImport.source_file == source_file)
    with SessionLocal() as session:
        return int(session.execute(stmt).scalar() or 0) > 0


def record_flow_import(source_file: str, rows: list[WanFlowUsageRecord], skipped_count: int = 0) -> int:
    'Persist imported WAN flow rows and mark the source file as imported.'
    with SessionLocal() as session:
        if session.execute(
            select(func.count()).select_from(FlowImport).where(FlowImport.source_file == source_file)
        ).scalar():
            return 0

        flow_rows = [
            WanFlowUsage(
                source_file=row.source_file,
                started_at=row.started_at,
                ended_at=row.ended_at,
                duration_seconds=row.duration_seconds,
                proto=row.proto,
                src_ip=row.src_ip,
                src_port=row.src_port,
                dst_ip=row.dst_ip,
                dst_port=row.dst_port,
                packets=row.packets,
                bytes=row.bytes,
                direction=row.direction,
                client_ip=row.client_ip,
            )
            for row in rows
        ]
        session.add_all(flow_rows)
        session.add(
            FlowImport(
                source_file=source_file,
                record_count=len(rows),
                skipped_count=skipped_count,
            )
        )
        session.commit()
        return len(flow_rows)


def get_wan_usage_by_client(
    period_start: datetime | None = None,
    period_end: datetime | None = None,
    limit: int = 100,
) -> list[WanClientUsageSummary]:
    'Return WAN upload/download totals grouped by internal client IP.'
    stmt = (
        select(
            WanFlowUsage.client_ip,
            func.sum(case((WanFlowUsage.direction == 'upload', WanFlowUsage.bytes), else_=0)),
            func.sum(case((WanFlowUsage.direction == 'download', WanFlowUsage.bytes), else_=0)),
            func.count(),
        )
        .group_by(WanFlowUsage.client_ip)
        .order_by(func.sum(WanFlowUsage.bytes).desc())
        .limit(max(1, limit))
    )
    if period_start is not None:
        stmt = stmt.where(WanFlowUsage.started_at >= period_start)
    if period_end is not None:
        stmt = stmt.where(WanFlowUsage.started_at <= period_end)

    with SessionLocal() as session:
        rows = session.execute(stmt).all()

    return [
        WanClientUsageSummary(
            client_ip=str(client_ip),
            upload_bytes=int(upload_bytes or 0),
            download_bytes=int(download_bytes or 0),
            flow_count=int(flow_count or 0),
        )
        for client_ip, upload_bytes, download_bytes, flow_count in rows
    ]


def get_wan_usage_by_client_ips(
    ip_addresses: list[str],
    period_start: datetime | None = None,
    period_end: datetime | None = None,
) -> dict[str, WanClientUsageSummary]:
    'Return WAN upload/download totals for requested internal client IPs.'
    requested_ips = sorted({ip_address for ip_address in ip_addresses if ip_address})
    if not requested_ips:
        return {}

    stmt = (
        select(
            WanFlowUsage.client_ip,
            func.sum(case((WanFlowUsage.direction == 'upload', WanFlowUsage.bytes), else_=0)),
            func.sum(case((WanFlowUsage.direction == 'download', WanFlowUsage.bytes), else_=0)),
            func.count(),
        )
        .where(WanFlowUsage.client_ip.in_(requested_ips))
        .group_by(WanFlowUsage.client_ip)
    )
    if period_start is not None:
        stmt = stmt.where(WanFlowUsage.started_at >= period_start)
    if period_end is not None:
        stmt = stmt.where(WanFlowUsage.started_at <= period_end)

    with SessionLocal() as session:
        rows = session.execute(stmt).all()

    return {
        str(client_ip): WanClientUsageSummary(
            client_ip=str(client_ip),
            upload_bytes=int(upload_bytes or 0),
            download_bytes=int(download_bytes or 0),
            flow_count=int(flow_count or 0),
        )
        for client_ip, upload_bytes, download_bytes, flow_count in rows
    }


def get_wan_usage_by_identity(
    period_start: datetime,
    period_end: datetime,
    identity_after_tolerance: timedelta = timedelta(minutes=10),
) -> list[WanIdentityUsageSummary]:
    'Return WAN totals grouped by client identity observed near each flow timestamp.'
    flow_stmt = (
        select(
            WanFlowUsage.started_at,
            WanFlowUsage.client_ip,
            WanFlowUsage.direction,
            WanFlowUsage.bytes,
        )
        .where(
            WanFlowUsage.started_at >= period_start,
            WanFlowUsage.started_at <= period_end,
        )
        .order_by(WanFlowUsage.started_at.asc(), WanFlowUsage.id.asc())
    )

    with SessionLocal() as session:
        flow_rows = session.execute(flow_stmt).all()

    client_ips = sorted({str(client_ip) for _, client_ip, _, _ in flow_rows if client_ip})
    if not client_ips:
        return []

    identity_stmt = (
        select(ClientIpIdentity)
        .where(
            ClientIpIdentity.ip_address.in_(client_ips),
            ClientIpIdentity.observed_at >= period_start - timedelta(days=1),
            ClientIpIdentity.observed_at <= period_end + identity_after_tolerance,
        )
        .order_by(ClientIpIdentity.ip_address.asc(), ClientIpIdentity.observed_at.asc(), ClientIpIdentity.id.asc())
    )

    identities_by_ip: dict[str, list[ClientIpIdentityRecord]] = {client_ip: [] for client_ip in client_ips}
    with SessionLocal() as session:
        for row in session.execute(identity_stmt).scalars():
            identities_by_ip.setdefault(row.ip_address, []).append(
                ClientIpIdentityRecord(
                    observed_at=row.observed_at,
                    ip_address=row.ip_address,
                    mac=row.mac,
                    name=row.name,
                    user_id=row.user_id,
                    vlan=row.vlan,
                )
            )

    observed_times_by_ip = {
        client_ip: [identity.observed_at for identity in identities]
        for client_ip, identities in identities_by_ip.items()
    }

    summary_by_key: dict[tuple[str, str, str, str, str], dict[str, Any]] = {}
    for started_at, client_ip, direction, byte_count in flow_rows:
        ip_text = str(client_ip)
        identities = identities_by_ip.get(ip_text, [])
        observed_times = observed_times_by_ip.get(ip_text, [])
        identity: ClientIpIdentityRecord | None = None
        if identities and observed_times:
            prior_index = bisect_right(observed_times, started_at) - 1
            if prior_index >= 0:
                identity = identities[prior_index]
            elif observed_times[0] <= started_at + identity_after_tolerance:
                identity = identities[0]

        mac = identity.mac if identity else ''
        name = identity.name if identity else ''
        user_id = identity.user_id if identity else ''
        vlan = identity.vlan if identity else 'Unknown'
        group_key = (mac or f'ip:{ip_text}', ip_text, name, user_id, vlan)
        summary = summary_by_key.setdefault(
            group_key,
            {
                'client_ip': ip_text,
                'mac': mac,
                'name': name,
                'user_id': user_id,
                'vlan': vlan,
                'upload_bytes': 0,
                'download_bytes': 0,
                'flow_count': 0,
            },
        )
        if str(direction) == 'upload':
            summary['upload_bytes'] = int(summary['upload_bytes']) + int(byte_count or 0)
        else:
            summary['download_bytes'] = int(summary['download_bytes']) + int(byte_count or 0)
        summary['flow_count'] = int(summary['flow_count']) + 1

    summaries = [
        WanIdentityUsageSummary(
            client_ip=str(summary['client_ip']),
            mac=str(summary['mac']),
            name=str(summary['name']),
            user_id=str(summary['user_id']),
            vlan=str(summary['vlan']),
            upload_bytes=int(summary['upload_bytes']),
            download_bytes=int(summary['download_bytes']),
            flow_count=int(summary['flow_count']),
        )
        for summary in summary_by_key.values()
    ]
    return sorted(
        summaries,
        key=lambda summary: summary.upload_bytes + summary.download_bytes,
        reverse=True,
    )


def get_wan_usage_by_identity_for_periods(
    period_starts: dict[str, datetime],
    period_end: datetime,
    identity_after_tolerance: timedelta = timedelta(minutes=10),
) -> dict[str, list[WanIdentityUsageSummary]]:
    'Return WAN identity totals for multiple periods using one flow/identity pass.'
    resolved_period_starts = {
        key: value
        for key, value in period_starts.items()
        if key and isinstance(value, datetime)
    }
    if not resolved_period_starts:
        return {}

    earliest_start = min(resolved_period_starts.values())
    flow_stmt = (
        select(
            WanFlowUsage.started_at,
            WanFlowUsage.client_ip,
            WanFlowUsage.direction,
            WanFlowUsage.bytes,
        )
        .where(
            WanFlowUsage.started_at >= earliest_start,
            WanFlowUsage.started_at <= period_end,
        )
        .order_by(WanFlowUsage.started_at.asc(), WanFlowUsage.id.asc())
    )

    with SessionLocal() as session:
        flow_rows = session.execute(flow_stmt).all()

    client_ips = sorted({str(client_ip) for _, client_ip, _, _ in flow_rows if client_ip})
    if not client_ips:
        return {key: [] for key in resolved_period_starts}

    identity_stmt = (
        select(ClientIpIdentity)
        .where(
            ClientIpIdentity.ip_address.in_(client_ips),
            ClientIpIdentity.observed_at >= earliest_start - timedelta(days=1),
            ClientIpIdentity.observed_at <= period_end + identity_after_tolerance,
        )
        .order_by(ClientIpIdentity.ip_address.asc(), ClientIpIdentity.observed_at.asc(), ClientIpIdentity.id.asc())
    )

    identities_by_ip: dict[str, list[ClientIpIdentityRecord]] = {client_ip: [] for client_ip in client_ips}
    with SessionLocal() as session:
        for row in session.execute(identity_stmt).scalars():
            identities_by_ip.setdefault(row.ip_address, []).append(
                ClientIpIdentityRecord(
                    observed_at=row.observed_at,
                    ip_address=row.ip_address,
                    mac=row.mac,
                    name=row.name,
                    user_id=row.user_id,
                    vlan=row.vlan,
                )
            )

    observed_times_by_ip = {
        client_ip: [identity.observed_at for identity in identities]
        for client_ip, identities in identities_by_ip.items()
    }
    summaries_by_period: dict[str, dict[tuple[str, str, str, str, str], dict[str, Any]]] = {
        key: {}
        for key in resolved_period_starts
    }

    for started_at, client_ip, direction, byte_count in flow_rows:
        ip_text = str(client_ip)
        identities = identities_by_ip.get(ip_text, [])
        observed_times = observed_times_by_ip.get(ip_text, [])
        identity: ClientIpIdentityRecord | None = None
        if identities and observed_times:
            prior_index = bisect_right(observed_times, started_at) - 1
            if prior_index >= 0:
                identity = identities[prior_index]
            elif observed_times[0] <= started_at + identity_after_tolerance:
                identity = identities[0]

        mac = identity.mac if identity else ''
        name = identity.name if identity else ''
        user_id = identity.user_id if identity else ''
        vlan = identity.vlan if identity else 'Unknown'
        group_key = (mac or f'ip:{ip_text}', ip_text, name, user_id, vlan)
        for period_key, period_start in resolved_period_starts.items():
            if started_at < period_start:
                continue
            summary = summaries_by_period[period_key].setdefault(
                group_key,
                {
                    'client_ip': ip_text,
                    'mac': mac,
                    'name': name,
                    'user_id': user_id,
                    'vlan': vlan,
                    'upload_bytes': 0,
                    'download_bytes': 0,
                    'flow_count': 0,
                },
            )
            if str(direction) == 'upload':
                summary['upload_bytes'] = int(summary['upload_bytes']) + int(byte_count or 0)
            else:
                summary['download_bytes'] = int(summary['download_bytes']) + int(byte_count or 0)
            summary['flow_count'] = int(summary['flow_count']) + 1

    result: dict[str, list[WanIdentityUsageSummary]] = {}
    for period_key, summaries in summaries_by_period.items():
        result[period_key] = sorted(
            [
                WanIdentityUsageSummary(
                    client_ip=str(summary['client_ip']),
                    mac=str(summary['mac']),
                    name=str(summary['name']),
                    user_id=str(summary['user_id']),
                    vlan=str(summary['vlan']),
                    upload_bytes=int(summary['upload_bytes']),
                    download_bytes=int(summary['download_bytes']),
                    flow_count=int(summary['flow_count']),
                )
                for summary in summaries.values()
            ],
            key=lambda summary: summary.upload_bytes + summary.download_bytes,
            reverse=True,
        )
    return result


def get_total_wan_usage(
    period_start: datetime,
    period_end: datetime,
) -> float:
    'Return total WAN flow usage in decimal MB for one period.'
    stmt = select(func.sum(WanFlowUsage.bytes)).where(
        WanFlowUsage.started_at >= period_start,
        WanFlowUsage.started_at <= period_end,
    )
    with SessionLocal() as session:
        total_bytes = int(session.execute(stmt).scalar() or 0)
    return total_bytes / 1_000_000.0


def get_global_daily_wan_usage_by_vlan(
    period_start: datetime,
    period_end: datetime,
    identity_after_tolerance: timedelta = timedelta(minutes=10),
) -> list[GlobalDailyWanVlanUsage]:
    'Return daily WAN MB series grouped by VLAN identity.'
    _, _, month_start, month_end = _month_period_bounds(period_start, period_end)
    flow_stmt = (
        select(
            WanFlowUsage.started_at,
            WanFlowUsage.client_ip,
            WanFlowUsage.bytes,
        )
        .where(
            WanFlowUsage.started_at >= period_start,
            WanFlowUsage.started_at <= period_end,
        )
        .order_by(WanFlowUsage.started_at.asc(), WanFlowUsage.id.asc())
    )

    with SessionLocal() as session:
        flow_rows = session.execute(flow_stmt).all()

    client_ips = sorted({str(client_ip) for _, client_ip, _ in flow_rows if client_ip})
    if not client_ips:
        return []

    identity_stmt = (
        select(ClientIpIdentity)
        .where(
            ClientIpIdentity.ip_address.in_(client_ips),
            ClientIpIdentity.observed_at >= period_start - timedelta(days=1),
            ClientIpIdentity.observed_at <= period_end + identity_after_tolerance,
        )
        .order_by(ClientIpIdentity.ip_address.asc(), ClientIpIdentity.observed_at.asc(), ClientIpIdentity.id.asc())
    )

    identities_by_ip: dict[str, list[ClientIpIdentityRecord]] = {client_ip: [] for client_ip in client_ips}
    with SessionLocal() as session:
        for row in session.execute(identity_stmt).scalars():
            identities_by_ip.setdefault(row.ip_address, []).append(
                ClientIpIdentityRecord(
                    observed_at=row.observed_at,
                    ip_address=row.ip_address,
                    mac=row.mac,
                    name=row.name,
                    user_id=row.user_id,
                    vlan=row.vlan,
                )
            )

    observed_times_by_ip = {
        client_ip: [identity.observed_at for identity in identities]
        for client_ip, identities in identities_by_ip.items()
    }
    totals_by_vlan_day: dict[str, dict[date, float]] = {}
    for started_at, client_ip, byte_count in flow_rows:
        if not isinstance(started_at, datetime):
            continue
        ip_text = str(client_ip)
        identities = identities_by_ip.get(ip_text, [])
        observed_times = observed_times_by_ip.get(ip_text, [])
        identity: ClientIpIdentityRecord | None = None
        if identities and observed_times:
            prior_index = bisect_right(observed_times, started_at) - 1
            if prior_index >= 0:
                identity = identities[prior_index]
            elif observed_times[0] <= started_at + identity_after_tolerance:
                identity = identities[0]

        vlan = identity.vlan if identity and identity.vlan else 'Unknown'
        usage_day = started_at.date()
        vlan_totals = totals_by_vlan_day.setdefault(vlan, {})
        vlan_totals[usage_day] = vlan_totals.get(usage_day, 0.0) + (int(byte_count or 0) / 1_000_000.0)

    day_count = (month_end - month_start).days + 1
    return [
        GlobalDailyWanVlanUsage(
            vlan=vlan,
            daily_mb=[
                totals_by_day.get(month_start + timedelta(days=offset), 0.0)
                for offset in range(day_count)
            ],
        )
        for vlan, totals_by_day in sorted(
            totals_by_vlan_day.items(),
            key=lambda item: (sum(item[1].values()), item[0].lower()),
            reverse=True,
        )
    ]


def get_wan_activity_series_by_mac(
    macs: list[str],
    buckets: int = 12,
    bucket_seconds: int = 300,
    period_end: datetime | None = None,
    identity_after_tolerance: timedelta = timedelta(minutes=10),
) -> dict[str, list[float]]:
    'Return per-client WAN MB buckets ordered oldest to newest.'
    target_macs = sorted({mac.lower() for mac in macs if mac})
    if not target_macs or buckets <= 0 or bucket_seconds <= 0:
        return {}

    resolved_period_end = period_end or datetime.now()
    period_start = resolved_period_end - timedelta(seconds=buckets * bucket_seconds)
    series_by_mac = {mac: [0.0] * buckets for mac in target_macs}

    flow_stmt = (
        select(
            WanFlowUsage.started_at,
            WanFlowUsage.client_ip,
            WanFlowUsage.bytes,
        )
        .where(
            WanFlowUsage.started_at >= period_start,
            WanFlowUsage.started_at <= resolved_period_end,
        )
        .order_by(WanFlowUsage.started_at.asc(), WanFlowUsage.id.asc())
    )

    with SessionLocal() as session:
        flow_rows = session.execute(flow_stmt).all()

    client_ips = sorted({str(client_ip) for _, client_ip, _ in flow_rows if client_ip})
    if not client_ips:
        return series_by_mac

    identity_stmt = (
        select(ClientIpIdentity)
        .where(
            ClientIpIdentity.ip_address.in_(client_ips),
            ClientIpIdentity.observed_at >= period_start - timedelta(days=1),
            ClientIpIdentity.observed_at <= resolved_period_end + identity_after_tolerance,
        )
        .order_by(ClientIpIdentity.ip_address.asc(), ClientIpIdentity.observed_at.asc(), ClientIpIdentity.id.asc())
    )

    identities_by_ip: dict[str, list[ClientIpIdentityRecord]] = {client_ip: [] for client_ip in client_ips}
    with SessionLocal() as session:
        for row in session.execute(identity_stmt).scalars():
            identities_by_ip.setdefault(row.ip_address, []).append(
                ClientIpIdentityRecord(
                    observed_at=row.observed_at,
                    ip_address=row.ip_address,
                    mac=row.mac,
                    name=row.name,
                    user_id=row.user_id,
                    vlan=row.vlan,
                )
            )

    observed_times_by_ip = {
        client_ip: [identity.observed_at for identity in identities]
        for client_ip, identities in identities_by_ip.items()
    }

    for started_at, client_ip, byte_count in flow_rows:
        if not isinstance(started_at, datetime):
            continue
        ip_text = str(client_ip)
        identities = identities_by_ip.get(ip_text, [])
        observed_times = observed_times_by_ip.get(ip_text, [])
        identity: ClientIpIdentityRecord | None = None
        if identities and observed_times:
            prior_index = bisect_right(observed_times, started_at) - 1
            if prior_index >= 0:
                identity = identities[prior_index]
            elif observed_times[0] <= started_at + identity_after_tolerance:
                identity = identities[0]
        if identity is None:
            continue

        mac_key = identity.mac.lower()
        if mac_key not in series_by_mac:
            continue

        bucket_index = int((started_at - period_start).total_seconds() // bucket_seconds)
        if bucket_index < 0:
            bucket_index = 0
        elif bucket_index >= buckets:
            bucket_index = buckets - 1
        series_by_mac[mac_key][bucket_index] += int(byte_count or 0) / 1_000_000.0

    return series_by_mac


def get_wan_usage_summary_for_user_id(
    user_id: str | int,
    period_start: datetime,
    period_end: datetime | None = None,
    identity_after_tolerance: timedelta = timedelta(minutes=10),
) -> tuple[datetime | None, float]:
    'Return first WAN flow time and MB attributed to one RADIUS user ID.'
    target_user_id = str(user_id).strip()
    if not target_user_id:
        return None, 0.0

    resolved_period_end = period_end or datetime.now()
    flow_stmt = (
        select(
            WanFlowUsage.started_at,
            WanFlowUsage.client_ip,
            WanFlowUsage.bytes,
        )
        .where(
            WanFlowUsage.started_at >= period_start,
            WanFlowUsage.started_at <= resolved_period_end,
        )
        .order_by(WanFlowUsage.started_at.asc(), WanFlowUsage.id.asc())
    )

    with SessionLocal() as session:
        flow_rows = session.execute(flow_stmt).all()

    client_ips = sorted({str(client_ip) for _, client_ip, _ in flow_rows if client_ip})
    if not client_ips:
        return None, 0.0

    identity_stmt = (
        select(ClientIpIdentity)
        .where(
            ClientIpIdentity.ip_address.in_(client_ips),
            ClientIpIdentity.observed_at >= period_start - timedelta(days=1),
            ClientIpIdentity.observed_at <= resolved_period_end + identity_after_tolerance,
        )
        .order_by(ClientIpIdentity.ip_address.asc(), ClientIpIdentity.observed_at.asc(), ClientIpIdentity.id.asc())
    )

    identities_by_ip: dict[str, list[ClientIpIdentityRecord]] = {client_ip: [] for client_ip in client_ips}
    with SessionLocal() as session:
        for row in session.execute(identity_stmt).scalars():
            identities_by_ip.setdefault(row.ip_address, []).append(
                ClientIpIdentityRecord(
                    observed_at=row.observed_at,
                    ip_address=row.ip_address,
                    mac=row.mac,
                    name=row.name,
                    user_id=row.user_id,
                    vlan=row.vlan,
                )
            )

    observed_times_by_ip = {
        client_ip: [identity.observed_at for identity in identities]
        for client_ip, identities in identities_by_ip.items()
    }

    first_usage_at: datetime | None = None
    total_bytes = 0
    for started_at, client_ip, byte_count in flow_rows:
        ip_text = str(client_ip)
        identities = identities_by_ip.get(ip_text, [])
        observed_times = observed_times_by_ip.get(ip_text, [])
        identity: ClientIpIdentityRecord | None = None
        if identities and observed_times:
            prior_index = bisect_right(observed_times, started_at) - 1
            if prior_index >= 0:
                identity = identities[prior_index]
            elif observed_times[0] <= started_at + identity_after_tolerance:
                identity = identities[0]

        if identity is None or identity.user_id.strip() != target_user_id:
            continue

        if first_usage_at is None:
            first_usage_at = started_at
        total_bytes += int(byte_count or 0)

    return first_usage_at, total_bytes / 1_000_000.0


def get_wan_flow_rows_for_mac(
    mac: str,
    period_start: datetime,
    period_end: datetime,
    identity_after_tolerance: timedelta = timedelta(minutes=10),
) -> list[WanMacFlowUsage]:
    'Return WAN flow timestamps and bytes attributed to one client MAC.'
    target_mac = mac.lower()
    if not target_mac:
        return []

    identity_window_start = period_start - timedelta(days=1)
    identity_window_end = period_end + identity_after_tolerance
    target_identity_stmt = (
        select(ClientIpIdentity)
        .where(
            ClientIpIdentity.mac == target_mac,
            ClientIpIdentity.observed_at >= identity_window_start,
            ClientIpIdentity.observed_at <= identity_window_end,
        )
        .order_by(ClientIpIdentity.ip_address.asc(), ClientIpIdentity.observed_at.asc(), ClientIpIdentity.id.asc())
    )

    with SessionLocal() as session:
        target_identity_rows = session.execute(target_identity_stmt).scalars().all()

    client_ips = sorted({row.ip_address for row in target_identity_rows if row.ip_address})
    if not client_ips:
        return []

    flow_stmt = (
        select(
            WanFlowUsage.source_file,
            WanFlowUsage.started_at,
            WanFlowUsage.client_ip,
            WanFlowUsage.direction,
            WanFlowUsage.bytes,
        )
        .where(
            WanFlowUsage.client_ip.in_(client_ips),
            WanFlowUsage.started_at >= period_start,
            WanFlowUsage.started_at <= period_end,
        )
        .order_by(WanFlowUsage.started_at.asc(), WanFlowUsage.id.asc())
    )

    with SessionLocal() as session:
        flow_rows = session.execute(flow_stmt).all()

    identity_stmt = (
        select(ClientIpIdentity)
        .where(
            ClientIpIdentity.ip_address.in_(client_ips),
            ClientIpIdentity.observed_at >= identity_window_start,
            ClientIpIdentity.observed_at <= identity_window_end,
        )
        .order_by(ClientIpIdentity.ip_address.asc(), ClientIpIdentity.observed_at.asc(), ClientIpIdentity.id.asc())
    )

    identities_by_ip: dict[str, list[ClientIpIdentityRecord]] = {client_ip: [] for client_ip in client_ips}
    with SessionLocal() as session:
        for row in session.execute(identity_stmt).scalars():
            identities_by_ip.setdefault(row.ip_address, []).append(
                ClientIpIdentityRecord(
                    observed_at=row.observed_at,
                    ip_address=row.ip_address,
                    mac=row.mac,
                    name=row.name,
                    user_id=row.user_id,
                    vlan=row.vlan,
                )
            )

    observed_times_by_ip = {
        client_ip: [identity.observed_at for identity in identities]
        for client_ip, identities in identities_by_ip.items()
    }

    attributed_rows: list[WanMacFlowUsage] = []
    for source_file, started_at, client_ip, direction, byte_count in flow_rows:
        ip_text = str(client_ip)
        identities = identities_by_ip.get(ip_text, [])
        observed_times = observed_times_by_ip.get(ip_text, [])
        identity: ClientIpIdentityRecord | None = None
        if identities and observed_times:
            prior_index = bisect_right(observed_times, started_at) - 1
            if prior_index >= 0:
                identity = identities[prior_index]
            elif observed_times[0] <= started_at + identity_after_tolerance:
                identity = identities[0]

        if identity is None or identity.mac.lower() != target_mac:
            continue

        attributed_rows.append(
            WanMacFlowUsage(
                source_file=str(source_file),
                started_at=started_at,
                bytes=int(byte_count or 0),
                direction=str(direction or ''),
            )
        )

    return attributed_rows


def get_wan_daily_totals_for_mac(
    mac: str,
    period_start: datetime,
    period_end: datetime,
) -> dict[date, float]:
    'Return WAN-attributed MB totals by day for one client MAC.'
    totals_by_day: dict[date, float] = {}
    for flow in get_wan_flow_rows_for_mac(mac, period_start, period_end):
        usage_day = flow.started_at.date()
        totals_by_day[usage_day] = totals_by_day.get(usage_day, 0.0) + flow.bytes / 1_000_000.0
    return totals_by_day


def get_wan_hourly_totals_for_mac(
    mac: str,
    period_start: datetime,
    period_end: datetime,
) -> dict[int, float]:
    'Return WAN-attributed MB totals by hour for one client MAC.'
    totals_by_hour: dict[int, float] = {}
    for flow in get_wan_flow_rows_for_mac(mac, period_start, period_end):
        usage_hour = flow.started_at.hour
        totals_by_hour[usage_hour] = totals_by_hour.get(usage_hour, 0.0) + flow.bytes / 1_000_000.0
    return totals_by_hour


def get_wan_usage_summary_for_mac(
    mac: str,
    period_start: datetime,
    period_end: datetime | None = None,
) -> tuple[datetime | None, float]:
    'Return first WAN flow time and MB attributed to one client MAC.'
    resolved_period_end = period_end or datetime.now()
    flow_rows = get_wan_flow_rows_for_mac(mac, period_start, resolved_period_end)
    if not flow_rows:
        return None, 0.0

    first_usage_at = min(flow.started_at for flow in flow_rows)
    total_bytes = sum(flow.bytes for flow in flow_rows)
    return first_usage_at, total_bytes / 1_000_000.0


def get_first_wan_flow_time() -> datetime | None:
    'Return the earliest imported WAN flow timestamp.'
    stmt = select(func.min(WanFlowUsage.started_at))
    with SessionLocal() as session:
        return session.execute(stmt).scalar()


def get_flow_import_times_by_source_file(source_files: set[str]) -> dict[str, datetime]:
    'Return import timestamps for source files that have completed ingestion.'
    normalized_sources = sorted(source_file for source_file in source_files if source_file)
    if not normalized_sources:
        return {}

    source_file_column = FlowImport.__table__.columns['source_file']
    imported_at_column = FlowImport.__table__.columns['imported_at']
    stmt = select(source_file_column, imported_at_column).where(
        or_(*(source_file_column == source_file for source_file in normalized_sources))
    )
    with SessionLocal() as session:
        return {
            str(source_file): imported_at
            for source_file, imported_at in session.execute(stmt).all()
        }


def get_recent_flow_imports(limit: int = 20) -> list[FlowImportRecord]:
    'Return recent nfdump capture imports, newest first.'
    stmt = (
        select(FlowImport)
        .order_by(FlowImport.imported_at.desc(), FlowImport.id.desc())
        .limit(max(1, limit))
    )

    with SessionLocal() as session:
        rows = session.execute(stmt).scalars().all()

    return [
        FlowImportRecord(
            source_file=row.source_file,
            imported_at=row.imported_at,
            record_count=row.record_count,
            skipped_count=row.skipped_count,
        )
        for row in rows
    ]


def record_client_ip_identities(clients: list[ClientInfo]) -> int:
    'Persist current UniFi IP-to-client identity observations.'
    identity_rows = [
        ClientIpIdentity(
            ip_address=client.ip_address,
            mac=client.mac.lower(),
            name=client.name or client.mac,
            user_id=client.user_id or '',
            vlan=client.vlan_name or '',
        )
        for client in clients
        if client.ip_address and client.mac
    ]
    if not identity_rows:
        return 0

    with SessionLocal() as session:
        session.add_all(identity_rows)
        session.commit()

    return len(identity_rows)


def get_latest_client_identities_by_ip(ip_addresses: list[str]) -> dict[str, ClientIpIdentityRecord]:
    'Return latest-known client identity for each requested IP address.'
    requested_ips = sorted({ip_address for ip_address in ip_addresses if ip_address})
    if not requested_ips:
        return {}

    stmt = (
        select(ClientIpIdentity)
        .where(ClientIpIdentity.ip_address.in_(requested_ips))
        .order_by(ClientIpIdentity.observed_at.desc(), ClientIpIdentity.id.desc())
    )

    identities_by_ip: dict[str, ClientIpIdentityRecord] = {}
    with SessionLocal() as session:
        rows = session.execute(stmt).scalars()
        for row in rows:
            if row.ip_address in identities_by_ip:
                continue
            identities_by_ip[row.ip_address] = ClientIpIdentityRecord(
                observed_at=row.observed_at,
                ip_address=row.ip_address,
                mac=row.mac,
                name=row.name,
                user_id=row.user_id,
                vlan=row.vlan,
            )
            if len(identities_by_ip) == len(requested_ips):
                break

    return identities_by_ip


def get_latest_client_identity_by_mac(mac: str) -> ClientIpIdentityRecord | None:
    'Return the latest-known IP identity observation for one MAC address.'
    normalized_mac = mac.lower()
    stmt = (
        select(ClientIpIdentity)
        .where(ClientIpIdentity.mac == normalized_mac)
        .order_by(ClientIpIdentity.observed_at.desc(), ClientIpIdentity.id.desc())
        .limit(1)
    )

    with SessionLocal() as session:
        row = session.execute(stmt).scalar_one_or_none()

    if row is None:
        return None

    return ClientIpIdentityRecord(
        observed_at=row.observed_at,
        ip_address=row.ip_address,
        mac=row.mac,
        name=row.name,
        user_id=row.user_id,
        vlan=row.vlan,
    )


def create_plus_vouchers(count: int, allocation_gb: int) -> list[PlusVoucherRecord]:
    'Create unconsumed Plus vouchers with unique active integer user IDs.'
    from voucher_repository import create_plus_vouchers as _create_plus_vouchers

    return _create_plus_vouchers(count, allocation_gb)


def get_plus_voucher_batch(batch_id: str) -> list[PlusVoucherRecord]:
    'Return all vouchers for one generated batch, ordered for printing.'
    from voucher_repository import get_plus_voucher_batch as _get_plus_voucher_batch

    return _get_plus_voucher_batch(batch_id)


def get_plus_vouchers(limit: int = 200) -> list[PlusVoucherRecord]:
    'Return recent Plus vouchers, newest first.'
    from voucher_repository import get_plus_vouchers as _get_plus_vouchers

    return _get_plus_vouchers(limit)


def get_unconsumed_plus_voucher_count() -> int:
    'Return the number of generated vouchers that have not been consumed.'
    from voucher_repository import get_unconsumed_plus_voucher_count as _get_unconsumed_plus_voucher_count

    return _get_unconsumed_plus_voucher_count()


def get_active_plus_voucher_for_user_id(user_id: str | int | None) -> PlusVoucherRecord | None:
    'Return the unconsumed voucher matching one RADIUS user ID.'
    from voucher_repository import get_active_plus_voucher_for_user_id as _get_active_plus_voucher_for_user_id

    return _get_active_plus_voucher_for_user_id(user_id)


def get_plus_voucher_usage_summary(voucher: PlusVoucherRecord) -> tuple[datetime | None, float]:
    'Return first usage time and lifetime usage for one voucher.'
    from voucher_repository import get_plus_voucher_usage_summary as _get_plus_voucher_usage_summary

    return _get_plus_voucher_usage_summary(voucher)


def get_active_plus_voucher_summaries() -> list[PlusVoucherUsageSummary]:
    'Return active voucher balances for admin review.'
    from voucher_repository import get_active_plus_voucher_summaries as _get_active_plus_voucher_summaries

    return _get_active_plus_voucher_summaries()


def log_usage(c: ClientInfo, interval_mb: float) -> None:
    'Persist one usage interval for a client.'
    with SessionLocal() as session:
        record = UsageRecord(
            user_id=c.user_id,
            mac=c.mac,
            name=c.name,
            vlan=c.vlan_name,
            mb_used=interval_mb,
            profile=c.speed_limit.name if c.speed_limit else None,
            ap_name=c.ap_name,
            signal=c.signal
        )
        session.add(record)
        session.commit()

def get_daily_total(mac: str) -> float:
    "Return one client's total usage for the current calendar day in MB."
    today_start = datetime.combine(datetime.now().date(), time.min)
    today_end = datetime.combine(datetime.now().date(), time.max)

    stmt = select(func.sum(UsageRecord.mb_used)).where(
        UsageRecord.mac == mac,
        UsageRecord.timestamp >= today_start,
        UsageRecord.timestamp <= today_end
    )

    with SessionLocal() as session:
        result = session.execute(stmt).scalar()
        return float(result or 0)


def get_last_7_days_total(mac: str) -> float:
    "Return one client's rolling 7-day usage total in MB."
    now = datetime.now()
    seven_days_ago = now - timedelta(days=7)

    stmt = select(func.sum(UsageRecord.mb_used)).where(
        UsageRecord.mac == mac,
        UsageRecord.timestamp >= seven_days_ago,
        UsageRecord.timestamp <= now,
    )

    with SessionLocal() as session:
        result = session.execute(stmt).scalar()
        return float(result or 0)


def get_calendar_month_total(mac: str) -> float:
    "Return one client's usage total since the start of this calendar month in MB."
    now = datetime.now()
    month_start = datetime.combine(now.date().replace(day=1), time.min)

    stmt = select(func.sum(UsageRecord.mb_used)).where(
        UsageRecord.mac == mac,
        UsageRecord.timestamp >= month_start,
        UsageRecord.timestamp <= now,
    )

    with SessionLocal() as session:
        result = session.execute(stmt).scalar()
        return float(result or 0)


def get_total_today_usage() -> float:
    'Return total usage across all clients for the current calendar day in MB.'
    today_start = datetime.combine(datetime.now().date(), time.min)
    today_end = datetime.combine(datetime.now().date(), time.max)

    stmt = select(func.sum(UsageRecord.mb_used)).where(
        UsageRecord.timestamp >= today_start,
        UsageRecord.timestamp <= today_end,
    )

    with SessionLocal() as session:
        result = session.execute(stmt).scalar()
        return float(result or 0)


def get_total_last_7_days_usage() -> float:
    'Return total usage across all clients for the rolling last 7 days in MB.'
    now = datetime.now()
    seven_days_ago = now - timedelta(days=7)

    stmt = select(func.sum(UsageRecord.mb_used)).where(
        UsageRecord.timestamp >= seven_days_ago,
        UsageRecord.timestamp <= now,
    )

    with SessionLocal() as session:
        result = session.execute(stmt).scalar()
        return float(result or 0)


def get_total_calendar_month_usage() -> float:
    'Return total usage across all clients since the start of this month in MB.'
    now = datetime.now()
    month_start = datetime.combine(now.date().replace(day=1), time.min)

    stmt = select(func.sum(UsageRecord.mb_used)).where(
        UsageRecord.timestamp >= month_start,
        UsageRecord.timestamp <= now,
    )

    with SessionLocal() as session:
        result = session.execute(stmt).scalar()
        return float(result or 0)


def get_recent_interval_totals(window_seconds: int = 90) -> dict[str, float]:
    'Return per-client MB totals over a recent time window.'
    now = datetime.now()
    window_start = now - timedelta(seconds=window_seconds)
    stmt = (
        select(UsageRecord.mac, func.sum(UsageRecord.mb_used))
        .where(
            UsageRecord.timestamp >= window_start,
            UsageRecord.timestamp <= now,
        )
        .group_by(UsageRecord.mac)
    )

    with SessionLocal() as session:
        rows = session.execute(stmt).all()

    return {str(mac): float(total or 0.0) for mac, total in rows}


def get_recent_activity_series(macs: list[str], buckets: int = 12, bucket_seconds: int = 60) -> dict[str, list[float]]:
    'Return per-client recent MB buckets ordered oldest to newest.'
    if not macs or buckets <= 0 or bucket_seconds <= 0:
        return {}

    now = datetime.now()
    window_start = now - timedelta(seconds=buckets * bucket_seconds)
    stmt = (
        select(UsageRecord.mac, UsageRecord.timestamp, UsageRecord.mb_used)
        .where(
            UsageRecord.timestamp >= window_start,
            UsageRecord.timestamp <= now,
            UsageRecord.mac.in_(macs),
        )
    )

    with SessionLocal() as session:
        rows = session.execute(stmt).all()

    series_by_mac = {mac: [0.0] * buckets for mac in macs}
    for mac, timestamp, mb_used in rows:
        if not isinstance(mac, str) or not isinstance(timestamp, datetime):
            continue

        delta_seconds = (timestamp - window_start).total_seconds()
        bucket_index = int(delta_seconds // bucket_seconds)
        if bucket_index < 0:
            bucket_index = 0
        elif bucket_index >= buckets:
            bucket_index = buckets - 1

        series_by_mac[mac][bucket_index] += float(mb_used or 0.0)

    return series_by_mac


def get_daily_usage_summary() -> list[DailyUsageSummary]:
    'Return per-client usage summaries for today, sorted by total descending.'
    today_start = datetime.combine(datetime.now().date(), time.min)
    today_end = datetime.combine(datetime.now().date(), time.max)
    stmt = (
        select(UsageRecord)
        .where(
            UsageRecord.timestamp >= today_start,
            UsageRecord.timestamp <= today_end,
        )
        .order_by(UsageRecord.timestamp.desc())
    )

    with SessionLocal() as session:
        records = session.execute(stmt).scalars().all()

    summary_by_mac: dict[str, DailyUsageSummary] = {}
    for record in records:
        existing = summary_by_mac.get(record.mac)
        if existing:
            summary_by_mac[record.mac] = DailyUsageSummary(
                mac=existing.mac,
                user_id=existing.user_id,
                name=existing.name,
                vlan=existing.vlan,
                profile=existing.profile,
                ap_name=existing.ap_name,
                signal=existing.signal,
                total_mb=existing.total_mb + record.mb_used,
                last_seen=existing.last_seen,
                usage_entries=existing.usage_entries + 1,
            )
            continue

        summary_by_mac[record.mac] = DailyUsageSummary(
            mac=record.mac,
            user_id=record.user_id,
            name=record.name,
            vlan=record.vlan,
            profile=record.profile,
            ap_name=record.ap_name,
            signal=record.signal,
            total_mb=record.mb_used,
            last_seen=record.timestamp,
            usage_entries=1,
        )

    return sorted(
        summary_by_mac.values(),
        key=lambda summary: summary.total_mb,
        reverse=True,
    )


def get_usage_history(mac: str, limit: int = 200) -> list[UsageRecord]:
    'Return most recent usage records for one client MAC.'
    stmt = (
        select(UsageRecord)
        .where(UsageRecord.mac == mac)
        .order_by(UsageRecord.timestamp.desc())
        .limit(limit)
    )

    with SessionLocal() as session:
        rows = session.execute(stmt).scalars().all()
        return [cast(UsageRecord, row) for row in rows]


def get_usage_window_summary(window: str) -> list[UsageWindowSummary]:
    'Return usage rollups for clients active in the requested dashboard time window.'
    now = datetime.now()
    today_start = datetime.combine(now.date(), time.min)
    seven_days_ago = now - timedelta(days=7)
    month_start = datetime.combine(now.date().replace(day=1), time.min)

    stmt = (
        select(UsageRecord)
        .where(
            UsageRecord.timestamp >= month_start,
            UsageRecord.timestamp <= now,
        )
        .order_by(UsageRecord.timestamp.desc())
    )
    with SessionLocal() as session:
        records = session.execute(stmt).scalars().all()

    summary_by_mac: dict[str, UsageWindowSummary] = {}
    for record in records:
        existing = summary_by_mac.get(record.mac)
        if existing:
            day_total_mb = existing.day_total_mb
            last_7_days_total_mb = existing.last_7_days_total_mb
            calendar_month_total_mb = existing.calendar_month_total_mb + record.mb_used
        else:
            day_total_mb = 0.0
            last_7_days_total_mb = 0.0
            calendar_month_total_mb = record.mb_used

        if record.timestamp >= today_start:
            day_total_mb += record.mb_used
        if record.timestamp >= seven_days_ago:
            last_7_days_total_mb += record.mb_used

        if existing:
            summary_by_mac[record.mac] = UsageWindowSummary(
                mac=existing.mac,
                user_id=existing.user_id,
                name=existing.name,
                vlan=existing.vlan,
                profile=existing.profile,
                ap_name=existing.ap_name,
                day_total_mb=day_total_mb,
                last_7_days_total_mb=last_7_days_total_mb,
                calendar_month_total_mb=calendar_month_total_mb,
                last_seen=existing.last_seen,
            )
            continue

        summary_by_mac[record.mac] = UsageWindowSummary(
            mac=record.mac,
            user_id=record.user_id,
            name=record.name,
            vlan=record.vlan,
            profile=record.profile,
            ap_name=record.ap_name,
            day_total_mb=day_total_mb,
            last_7_days_total_mb=last_7_days_total_mb,
            calendar_month_total_mb=calendar_month_total_mb,
            last_seen=record.timestamp,
        )

    summaries = list(summary_by_mac.values())
    if window == "today":
        return sorted(
            [row for row in summaries if row.day_total_mb > 0],
            key=lambda row: row.day_total_mb,
            reverse=True,
        )
    if window == "last_7_days":
        return sorted(
            [row for row in summaries if row.last_7_days_total_mb > 0],
            key=lambda row: row.last_7_days_total_mb,
            reverse=True,
        )

    return sorted(
        [row for row in summaries if row.calendar_month_total_mb > 0],
        key=lambda row: row.calendar_month_total_mb,
        reverse=True,
    )


def get_usage_summary_for_period(period_start: datetime, period_end: datetime) -> list[UsageWindowSummary]:
    'Return sampled UniFi usage rollups for an explicit comparison period.'
    stmt = (
        select(UsageRecord)
        .where(
            UsageRecord.timestamp >= period_start,
            UsageRecord.timestamp <= period_end,
        )
        .order_by(UsageRecord.timestamp.desc())
    )
    with SessionLocal() as session:
        records = session.execute(stmt).scalars().all()

    summary_by_mac: dict[str, UsageWindowSummary] = {}
    for record in records:
        existing = summary_by_mac.get(record.mac)
        if existing:
            summary_by_mac[record.mac] = UsageWindowSummary(
                mac=existing.mac,
                user_id=existing.user_id,
                name=existing.name,
                vlan=existing.vlan,
                profile=existing.profile,
                ap_name=existing.ap_name,
                day_total_mb=existing.day_total_mb + record.mb_used,
                last_7_days_total_mb=existing.last_7_days_total_mb + record.mb_used,
                calendar_month_total_mb=existing.calendar_month_total_mb + record.mb_used,
                last_seen=existing.last_seen,
            )
            continue

        summary_by_mac[record.mac] = UsageWindowSummary(
            mac=record.mac,
            user_id=record.user_id,
            name=record.name,
            vlan=record.vlan,
            profile=record.profile,
            ap_name=record.ap_name,
            day_total_mb=record.mb_used,
            last_7_days_total_mb=record.mb_used,
            calendar_month_total_mb=record.mb_used,
            last_seen=record.timestamp,
        )

    return sorted(
        summary_by_mac.values(),
        key=lambda row: row.calendar_month_total_mb,
        reverse=True,
    )


def get_usage_window_access_point_minutes(window: str) -> dict[str, list[tuple[str, int]]]:
    'Return per-client AP minute counts for the requested dashboard window.'
    now = datetime.now()
    today_start = datetime.combine(now.date(), time.min)
    seven_days_ago = now - timedelta(days=7)
    month_start = datetime.combine(now.date().replace(day=1), time.min)

    window_start = month_start
    if window == 'today':
        window_start = today_start
    elif window == 'last_7_days':
        window_start = seven_days_ago

    stmt = (
        select(UsageRecord.mac, UsageRecord.ap_name)
        .where(
            UsageRecord.timestamp >= window_start,
            UsageRecord.timestamp <= now,
        )
        .order_by(UsageRecord.timestamp.asc())
    )
    with SessionLocal() as session:
        rows = session.execute(stmt).all()

    ap_minutes_by_mac: dict[str, dict[str, int]] = {}
    for row_mac, row_ap_name in rows:
        if not isinstance(row_mac, str):
            continue
        ap_name = row_ap_name.strip() if isinstance(row_ap_name, str) and row_ap_name.strip() else 'Unknown'
        if ap_name.lower().endswith(' ap'):
            ap_name = ap_name[:-3].rstrip() or 'Unknown'
        ap_counts = ap_minutes_by_mac.setdefault(row_mac, {})
        ap_counts[ap_name] = ap_counts.get(ap_name, 0) + 1

    return {
        mac: sorted(
            ap_counts.items(),
            key=lambda item: (item[1], item[0].lower()),
            reverse=True,
        )
        for mac, ap_counts in ap_minutes_by_mac.items()
    }


def get_calendar_month_daily_totals(mac: str) -> list[tuple[date, float, int]]:
    'Return per-day usage totals and active-minute counts for current month, oldest to newest.'
    now = datetime.now()
    month_start = date(now.year, now.month, 1)
    today = now.date()
    month_start_dt = datetime.combine(month_start, time.min)
    month_end_dt = datetime.combine(today, time.max)

    stmt = (
        select(UsageRecord.timestamp, UsageRecord.mb_used)
        .where(
            UsageRecord.mac == mac,
            UsageRecord.timestamp >= month_start_dt,
            UsageRecord.timestamp <= month_end_dt,
        )
        .order_by(UsageRecord.timestamp.asc())
    )

    with SessionLocal() as session:
        rows = session.execute(stmt).all()

    totals_by_day: dict[date, float] = {}
    active_minutes_by_day: dict[date, int] = {}
    for row_timestamp, row_mb_used in rows:
        if not isinstance(row_timestamp, datetime):
            continue
        usage_day = row_timestamp.date()
        totals_by_day[usage_day] = totals_by_day.get(usage_day, 0.0) + float(row_mb_used or 0.0)
        active_minutes_by_day[usage_day] = active_minutes_by_day.get(usage_day, 0) + 1

    day = month_start
    series: list[tuple[date, float, int]] = []
    while day <= today:
        series.append((day, totals_by_day.get(day, 0.0), active_minutes_by_day.get(day, 0)))
        day += timedelta(days=1)

    return series


def get_today_hourly_totals(mac: str) -> list[tuple[int, float, int]]:
    'Return per-hour usage totals and active-minute counts for today, oldest to newest.'
    now = datetime.now()
    today = now.date()
    today_start_dt = datetime.combine(today, time.min)
    today_end_dt = datetime.combine(today, time.max)

    stmt = (
        select(UsageRecord.timestamp, UsageRecord.mb_used)
        .where(
            UsageRecord.mac == mac,
            UsageRecord.timestamp >= today_start_dt,
            UsageRecord.timestamp <= today_end_dt,
        )
        .order_by(UsageRecord.timestamp.asc())
    )

    with SessionLocal() as session:
        rows = session.execute(stmt).all()

    totals_by_hour: dict[int, float] = {}
    active_minutes_by_hour: dict[int, int] = {}
    for row_timestamp, row_mb_used in rows:
        if not isinstance(row_timestamp, datetime):
            continue
        usage_hour = row_timestamp.hour
        totals_by_hour[usage_hour] = totals_by_hour.get(usage_hour, 0.0) + float(row_mb_used or 0.0)
        active_minutes_by_hour[usage_hour] = active_minutes_by_hour.get(usage_hour, 0) + 1

    series: list[tuple[int, float, int]] = []
    for hour in range(0, now.hour + 1):
        series.append((hour, totals_by_hour.get(hour, 0.0), active_minutes_by_hour.get(hour, 0)))

    return series


def get_calendar_month_daily_profile_minutes(mac: str) -> list[tuple[date, dict[str, int]]]:
    'Return per-day active-minute counts grouped by profile for current month.'
    now = datetime.now()
    month_start = date(now.year, now.month, 1)
    today = now.date()
    month_start_dt = datetime.combine(month_start, time.min)
    month_end_dt = datetime.combine(today, time.max)

    stmt = (
        select(UsageRecord.timestamp, UsageRecord.profile)
        .where(
            UsageRecord.mac == mac,
            UsageRecord.timestamp >= month_start_dt,
            UsageRecord.timestamp <= month_end_dt,
        )
        .order_by(UsageRecord.timestamp.asc())
    )

    with SessionLocal() as session:
        rows = session.execute(stmt).all()

    day_profile_counts: dict[date, dict[str, int]] = {}
    for row_timestamp, row_profile in rows:
        if not isinstance(row_timestamp, datetime):
            continue

        usage_day = row_timestamp.date()
        profile_key = row_profile.strip() if isinstance(row_profile, str) and row_profile.strip() else ''
        profile_counts = day_profile_counts.setdefault(usage_day, {})
        profile_counts[profile_key] = profile_counts.get(profile_key, 0) + 1

    day = month_start
    series: list[tuple[date, dict[str, int]]] = []
    while day <= today:
        series.append((day, day_profile_counts.get(day, {})))
        day += timedelta(days=1)

    return series


def get_today_hourly_profile_minutes(mac: str) -> list[tuple[int, dict[str, int]]]:
    'Return per-hour active-minute counts grouped by profile for today.'
    now = datetime.now()
    today = now.date()
    today_start_dt = datetime.combine(today, time.min)
    today_end_dt = datetime.combine(today, time.max)

    stmt = (
        select(UsageRecord.timestamp, UsageRecord.profile)
        .where(
            UsageRecord.mac == mac,
            UsageRecord.timestamp >= today_start_dt,
            UsageRecord.timestamp <= today_end_dt,
        )
        .order_by(UsageRecord.timestamp.asc())
    )

    with SessionLocal() as session:
        rows = session.execute(stmt).all()

    hour_profile_counts: dict[int, dict[str, int]] = {}
    for row_timestamp, row_profile in rows:
        if not isinstance(row_timestamp, datetime):
            continue

        usage_hour = row_timestamp.hour
        profile_key = row_profile.strip() if isinstance(row_profile, str) and row_profile.strip() else ''
        profile_counts = hour_profile_counts.setdefault(usage_hour, {})
        profile_counts[profile_key] = profile_counts.get(profile_key, 0) + 1

    series: list[tuple[int, dict[str, int]]] = []
    for hour in range(0, now.hour + 1):
        series.append((hour, hour_profile_counts.get(hour, {})))

    return series


def normalize_access_point_name(ap_name: object) -> str:
    'Return a compact display label for an access point value.'
    normalized_name = ap_name.strip() if isinstance(ap_name, str) and ap_name.strip() else 'Unknown'
    if normalized_name.lower().endswith(' ap'):
        normalized_name = normalized_name[:-3].rstrip() or 'Unknown'
    return normalized_name


def get_calendar_month_daily_access_point_minutes(mac: str) -> list[tuple[date, dict[str, int]]]:
    'Return per-day active-minute counts grouped by access point for current month.'
    now = datetime.now()
    month_start = date(now.year, now.month, 1)
    today = now.date()
    month_start_dt = datetime.combine(month_start, time.min)
    month_end_dt = datetime.combine(today, time.max)

    stmt = (
        select(UsageRecord.timestamp, UsageRecord.ap_name)
        .where(
            UsageRecord.mac == mac,
            UsageRecord.timestamp >= month_start_dt,
            UsageRecord.timestamp <= month_end_dt,
        )
        .order_by(UsageRecord.timestamp.asc())
    )

    with SessionLocal() as session:
        rows = session.execute(stmt).all()

    day_ap_counts: dict[date, dict[str, int]] = {}
    for row_timestamp, row_ap_name in rows:
        if not hasattr(row_timestamp, 'date'):
            continue

        usage_day = row_timestamp.date()
        ap_name = normalize_access_point_name(row_ap_name)
        ap_counts = day_ap_counts.setdefault(usage_day, {})
        ap_counts[ap_name] = ap_counts.get(ap_name, 0) + 1

    day = month_start
    series: list[tuple[date, dict[str, int]]] = []
    while day <= today:
        series.append((day, day_ap_counts.get(day, {})))
        day += timedelta(days=1)

    return series


def get_today_hourly_access_point_minutes(mac: str) -> list[tuple[int, dict[str, int]]]:
    'Return per-hour active-minute counts grouped by access point for today.'
    now = datetime.now()
    today = now.date()
    today_start_dt = datetime.combine(today, time.min)
    today_end_dt = datetime.combine(today, time.max)

    stmt = (
        select(UsageRecord.timestamp, UsageRecord.ap_name)
        .where(
            UsageRecord.mac == mac,
            UsageRecord.timestamp >= today_start_dt,
            UsageRecord.timestamp <= today_end_dt,
        )
        .order_by(UsageRecord.timestamp.asc())
    )

    with SessionLocal() as session:
        rows = session.execute(stmt).all()

    hour_ap_counts: dict[int, dict[str, int]] = {}
    for row_timestamp, row_ap_name in rows:
        if not hasattr(row_timestamp, 'hour'):
            continue

        usage_hour = row_timestamp.hour
        ap_name = normalize_access_point_name(row_ap_name)
        ap_counts = hour_ap_counts.setdefault(usage_hour, {})
        ap_counts[ap_name] = ap_counts.get(ap_name, 0) + 1

    series: list[tuple[int, dict[str, int]]] = []
    for hour in range(0, now.hour + 1):
        series.append((hour, hour_ap_counts.get(hour, {})))

    return series


def get_today_access_point_totals(mac: str) -> list[tuple[str, float, int]]:
    'Return today usage totals per access point for one client.'
    now = datetime.now()
    today = now.date()
    today_start_dt = datetime.combine(today, time.min)
    today_end_dt = datetime.combine(today, time.max)

    stmt = (
        select(UsageRecord.ap_name, UsageRecord.mb_used)
        .where(
            UsageRecord.mac == mac,
            UsageRecord.timestamp >= today_start_dt,
            UsageRecord.timestamp <= today_end_dt,
        )
        .order_by(UsageRecord.ap_name.asc())
    )

    with SessionLocal() as session:
        rows = session.execute(stmt).all()

    totals_by_ap: dict[str, tuple[float, int]] = {}
    for row_ap_name, row_mb_used in rows:
        ap_name = normalize_access_point_name(row_ap_name)
        existing_total_mb, existing_minutes = totals_by_ap.get(ap_name, (0.0, 0))
        totals_by_ap[ap_name] = (existing_total_mb + float(row_mb_used or 0.0), existing_minutes + 1)

    return sorted(
        ((ap_name, total_mb, active_minutes) for ap_name, (total_mb, active_minutes) in totals_by_ap.items()),
        key=lambda row: (row[2], row[1], row[0].lower()),
        reverse=True,
    )


def get_calendar_month_access_point_totals(mac: str) -> list[tuple[str, float, int]]:
    'Return month-to-date usage totals per access point for one client.'
    now = datetime.now()
    month_start = date(now.year, now.month, 1)
    today = now.date()
    month_start_dt = datetime.combine(month_start, time.min)
    month_end_dt = datetime.combine(today, time.max)

    stmt = (
        select(UsageRecord.ap_name, UsageRecord.mb_used)
        .where(
            UsageRecord.mac == mac,
            UsageRecord.timestamp >= month_start_dt,
            UsageRecord.timestamp <= month_end_dt,
        )
        .order_by(UsageRecord.ap_name.asc())
    )

    with SessionLocal() as session:
        rows = session.execute(stmt).all()

    totals_by_ap: dict[str, tuple[float, int]] = {}
    for row_ap_name, row_mb_used in rows:
        ap_name = normalize_access_point_name(row_ap_name)
        existing_total_mb, existing_minutes = totals_by_ap.get(ap_name, (0.0, 0))
        totals_by_ap[ap_name] = (existing_total_mb + float(row_mb_used or 0.0), existing_minutes + 1)

    return sorted(
        ((ap_name, total_mb, active_minutes) for ap_name, (total_mb, active_minutes) in totals_by_ap.items()),
        key=lambda row: (row[2], row[1], row[0].lower()),
        reverse=True,
    )


def get_global_month_insights(
    top_limit: int = 5,
    period_start: datetime | None = None,
    period_end: datetime | None = None,
) -> GlobalInsights:
    'Return global analytics for dashboard insights panels.'
    month_start_dt, month_end_dt, month_start, month_end = _month_period_bounds(period_start, period_end)

    stmt = (
        select(
            UsageRecord.timestamp,
            UsageRecord.mac,
            UsageRecord.name,
            UsageRecord.user_id,
            UsageRecord.mb_used,
            UsageRecord.ap_name,
        )
        .where(
            UsageRecord.timestamp >= month_start_dt,
            UsageRecord.timestamp <= month_end_dt,
        )
        .order_by(UsageRecord.timestamp.asc())
    )

    with SessionLocal() as session:
        rows = session.execute(stmt).all()

    daily_active_users: dict[date, set[str]] = {}
    user_totals: dict[str, tuple[float, int]] = {}
    user_latest_identity: dict[str, tuple[datetime, str, str]] = {}
    ap_totals: dict[str, tuple[float, int]] = {}

    for row_timestamp, row_mac, row_name, row_user_id, row_mb_used, row_ap_name in rows:
        if not isinstance(row_timestamp, datetime) or not isinstance(row_mac, str):
            continue

        usage_day = row_timestamp.date()
        daily_active_users.setdefault(usage_day, set()).add(row_mac)

        total_mb, active_minutes = user_totals.get(row_mac, (0.0, 0))
        user_totals[row_mac] = (total_mb + float(row_mb_used or 0.0), active_minutes + 1)

        resolved_name = (row_name.strip() if isinstance(row_name, str) and row_name.strip() else row_mac)
        resolved_user_id = (row_user_id.strip() if isinstance(row_user_id, str) and row_user_id.strip() else '')
        previous_identity = user_latest_identity.get(row_mac)
        if previous_identity is None or row_timestamp >= previous_identity[0]:
            user_latest_identity[row_mac] = (row_timestamp, resolved_name, resolved_user_id)

        ap_key = row_ap_name.strip() if isinstance(row_ap_name, str) and row_ap_name.strip() else 'Unknown'
        ap_total_mb, ap_active_minutes = ap_totals.get(ap_key, (0.0, 0))
        ap_totals[ap_key] = (ap_total_mb + float(row_mb_used or 0.0), ap_active_minutes + 1)

    day = month_start
    daily_counts: list[int] = []
    while day <= month_end:
        daily_counts.append(len(daily_active_users.get(day, set())))
        day += timedelta(days=1)

    if daily_counts:
        active_users_min = min(daily_counts)
        active_users_max = max(daily_counts)
        active_users_mean = sum(daily_counts) / len(daily_counts)
        active_users_today = daily_counts[-1]
    else:
        active_users_min = 0
        active_users_max = 0
        active_users_mean = 0.0
        active_users_today = 0

    top_users = sorted(
        (
            GlobalTopUser(
                mac=mac,
                name=user_latest_identity.get(mac, (datetime.min, mac, ''))[1],
                user_id=user_latest_identity.get(mac, (datetime.min, '', ''))[2],
                total_mb=totals[0],
                active_minutes=totals[1],
            )
            for mac, totals in user_totals.items()
        ),
        key=lambda row: (row.total_mb, row.active_minutes),
        reverse=True,
    )[:max(1, top_limit)]

    top_access_points = sorted(
        (
            GlobalTopAccessPoint(
                ap_name=ap_name.removesuffix(' AP'),
                total_mb=totals[0],
                active_minutes=totals[1],
            )
            for ap_name, totals in ap_totals.items()
        ),
        key=lambda row: (row.total_mb, row.active_minutes),
        reverse=True,
    )[:max(1, top_limit)]

    return GlobalInsights(
        active_users_min=active_users_min,
        active_users_mean=active_users_mean,
        active_users_max=active_users_max,
        active_users_today=active_users_today,
        days_in_period=len(daily_counts),
        active_users_daily_x_labels=[
            month_start.day + offset
            for offset in range(len(daily_counts))
        ],
        active_users_daily_full_labels=[
            f'{(month_start + timedelta(days=offset)).strftime("%b")} {(month_start + timedelta(days=offset)).day}'
            for offset in range(len(daily_counts))
        ],
        active_users_daily_counts=daily_counts,
        top_users=top_users,
        top_access_points=top_access_points,
    )


def get_global_daily_network_usage_current_month(
    period_start: datetime | None = None,
    period_end: datetime | None = None,
) -> list[GlobalDailyNetworkUsage]:
    'Return daily Basic/Plus usage totals (MB + active minutes) for a month period.'
    month_start_dt, month_end_dt, month_start, month_end = _month_period_bounds(period_start, period_end)

    stmt = (
        select(UsageRecord.timestamp, UsageRecord.vlan, UsageRecord.mb_used)
        .where(
            UsageRecord.timestamp >= month_start_dt,
            UsageRecord.timestamp <= month_end_dt,
        )
        .order_by(UsageRecord.timestamp.asc())
    )

    with SessionLocal() as session:
        rows = session.execute(stmt).all()

    totals_by_day: dict[date, DailyNetworkUsageBucket] = {}
    for row_timestamp, row_vlan, row_mb_used in rows:
        if not isinstance(row_timestamp, datetime):
            continue

        usage_day = row_timestamp.date()
        bucket = totals_by_day.setdefault(usage_day, DailyNetworkUsageBucket())

        vlan_label = row_vlan.strip().lower() if isinstance(row_vlan, str) and row_vlan.strip() else ''
        mb_used = float(row_mb_used or 0.0)
        if vlan_label == 'plus':
            bucket.plus_mb += mb_used
            bucket.plus_minutes += 1
        else:
            bucket.basic_mb += mb_used
            bucket.basic_minutes += 1

    day = month_start
    series: list[GlobalDailyNetworkUsage] = []
    while day <= month_end:
        bucket = totals_by_day.get(day, DailyNetworkUsageBucket())
        series.append(
            GlobalDailyNetworkUsage(
                usage_day=day,
                basic_mb=bucket.basic_mb,
                plus_mb=bucket.plus_mb,
                basic_minutes=bucket.basic_minutes,
                plus_minutes=bucket.plus_minutes,
            )
        )
        day += timedelta(days=1)

    return series


def _wan_hour_bucket_expression() -> Any:
    'Return a SQL expression that truncates WAN flow timestamps to the hour.'
    if engine.dialect.name == 'postgresql':
        return func.date_trunc('hour', WanFlowUsage.started_at)
    return func.strftime('%Y-%m-%d %H:00:00', WanFlowUsage.started_at)


def _coerce_hour_bucket(raw_bucket: object) -> datetime | None:
    'Return a datetime from a SQL hour bucket value.'
    if isinstance(raw_bucket, datetime):
        return raw_bucket.replace(minute=0, second=0, microsecond=0)
    if isinstance(raw_bucket, str):
        try:
            return datetime.strptime(raw_bucket, '%Y-%m-%d %H:%M:%S')
        except ValueError:
            return None
    return None


def get_global_wan_hourly_usage_current_month(
    period_start: datetime | None = None,
    period_end: datetime | None = None,
) -> list[GlobalWanHourlyUsage]:
    'Return hourly WAN usage totals for a month period.'
    month_start_dt, month_end_dt, _, _ = _month_period_bounds(period_start, period_end)
    bucket_expr = _wan_hour_bucket_expression().label('hour_bucket')
    stmt = (
        select(bucket_expr, func.sum(WanFlowUsage.bytes))
        .where(
            WanFlowUsage.started_at >= month_start_dt,
            WanFlowUsage.started_at <= month_end_dt,
        )
        .group_by(bucket_expr)
        .order_by(bucket_expr.asc())
    )

    with SessionLocal() as session:
        rows = session.execute(stmt).all()

    totals_by_hour: dict[datetime, float] = {}
    for raw_bucket, total_bytes in rows:
        bucket_start = _coerce_hour_bucket(raw_bucket)
        if bucket_start is None:
            continue
        totals_by_hour[bucket_start] = float(total_bytes or 0) / 1_000_000.0

    hour_cursor = month_start_dt.replace(minute=0, second=0, microsecond=0)
    end_hour = month_end_dt.replace(minute=0, second=0, microsecond=0)
    series: list[GlobalWanHourlyUsage] = []
    while hour_cursor <= end_hour:
        series.append(
            GlobalWanHourlyUsage(
                bucket_start=hour_cursor,
                total_mb=totals_by_hour.get(hour_cursor, 0.0),
            )
        )
        hour_cursor += timedelta(hours=1)

    return series


def get_global_payer_split_current_month(
    organization_paid_macs: set[str] | None = None,
    organization_paid_user_ids: set[str] | None = None,
    organization_paid_vlan_names: set[str] | None = None,
    period_start: datetime | None = None,
    period_end: datetime | None = None,
) -> GlobalPayerSplit:
    'Return period totals split into organization-paid vs user-paid activity.'
    month_start_dt, month_end_dt, _, _ = _month_period_bounds(period_start, period_end)

    mac_allowlist = {mac.strip().lower() for mac in (organization_paid_macs or set()) if mac.strip()}
    user_allowlist = {user_id.strip().lower() for user_id in (organization_paid_user_ids or set()) if user_id.strip()}
    vlan_allowlist = {vlan.strip().lower() for vlan in (organization_paid_vlan_names or set()) if vlan.strip()}

    stmt = (
        select(UsageRecord.mac, UsageRecord.user_id, UsageRecord.vlan, UsageRecord.mb_used)
        .where(
            UsageRecord.timestamp >= month_start_dt,
            UsageRecord.timestamp <= month_end_dt,
        )
    )

    with SessionLocal() as session:
        rows = session.execute(stmt).all()

    organization_paid_total_mb = 0.0
    organization_paid_minutes = 0
    user_paid_total_mb = 0.0
    user_paid_minutes = 0

    for row_mac, row_user_id, row_vlan, row_mb_used in rows:
        mac_value = row_mac.strip().lower() if isinstance(row_mac, str) and row_mac.strip() else ''
        user_value = row_user_id.strip().lower() if isinstance(row_user_id, str) and row_user_id.strip() else ''
        vlan_value = row_vlan.strip().lower() if isinstance(row_vlan, str) and row_vlan.strip() else ''
        mb_used = float(row_mb_used or 0.0)

        is_organization_paid = (
            vlan_value in vlan_allowlist
            or mac_value in mac_allowlist
            or user_value in user_allowlist
        )
        if is_organization_paid:
            organization_paid_total_mb += mb_used
            organization_paid_minutes += 1
        else:
            user_paid_total_mb += mb_used
            user_paid_minutes += 1

    return GlobalPayerSplit(
        organization_paid_total_mb=organization_paid_total_mb,
        organization_paid_minutes=organization_paid_minutes,
        user_paid_total_mb=user_paid_total_mb,
        user_paid_minutes=user_paid_minutes,
    )


def get_global_top_users_current_month(
    limit: int = 6,
    exclude_organization_paid_macs: set[str] | None = None,
    exclude_organization_paid_user_ids: set[str] | None = None,
    exclude_organization_paid_vlan_names: set[str] | None = None,
    period_start: datetime | None = None,
    period_end: datetime | None = None,
) -> list[GlobalTopUser]:
    'Return top users by MB for a period, with optional organization-paid exclusion.'
    month_start_dt, month_end_dt, _, _ = _month_period_bounds(period_start, period_end)

    mac_exclude = {mac.strip().lower() for mac in (exclude_organization_paid_macs or set()) if mac.strip()}
    user_exclude = {user_id.strip().lower() for user_id in (exclude_organization_paid_user_ids or set()) if user_id.strip()}
    vlan_exclude = {vlan.strip().lower() for vlan in (exclude_organization_paid_vlan_names or set()) if vlan.strip()}

    stmt = (
        select(UsageRecord.timestamp, UsageRecord.mac, UsageRecord.user_id, UsageRecord.name, UsageRecord.vlan, UsageRecord.mb_used)
        .where(
            UsageRecord.timestamp >= month_start_dt,
            UsageRecord.timestamp <= month_end_dt,
        )
    )

    with SessionLocal() as session:
        rows = session.execute(stmt).all()

    user_totals: dict[str, tuple[float, int]] = {}
    user_latest_identity: dict[str, tuple[datetime, str, str]] = {}

    for row_timestamp, row_mac, row_user_id, row_name, row_vlan, row_mb_used in rows:
        if not isinstance(row_timestamp, datetime) or not isinstance(row_mac, str):
            continue

        mac_value = row_mac.strip().lower() if row_mac.strip() else ''
        user_value = row_user_id.strip().lower() if isinstance(row_user_id, str) and row_user_id.strip() else ''
        vlan_value = row_vlan.strip().lower() if isinstance(row_vlan, str) and row_vlan.strip() else ''
        is_excluded = (
            vlan_value in vlan_exclude
            or mac_value in mac_exclude
            or user_value in user_exclude
        )
        if is_excluded:
            continue

        total_mb, active_minutes = user_totals.get(row_mac, (0.0, 0))
        user_totals[row_mac] = (total_mb + float(row_mb_used or 0.0), active_minutes + 1)

        resolved_name = row_name.strip() if isinstance(row_name, str) and row_name.strip() else row_mac
        resolved_user_id = row_user_id.strip() if isinstance(row_user_id, str) and row_user_id.strip() else ''
        previous_identity = user_latest_identity.get(row_mac)
        if previous_identity is None or row_timestamp >= previous_identity[0]:
            user_latest_identity[row_mac] = (row_timestamp, resolved_name, resolved_user_id)

    results = sorted(
        (
            GlobalTopUser(
                mac=mac,
                name=user_latest_identity.get(mac, (datetime.min, mac, ''))[1],
                user_id=user_latest_identity.get(mac, (datetime.min, '', ''))[2],
                vlan_name='',
                total_mb=totals[0],
                active_minutes=totals[1],
            )
            for mac, totals in user_totals.items()
        ),
        key=lambda row: (row.total_mb, row.active_minutes),
        reverse=True,
    )
    return results[:max(1, limit)]


def get_global_organization_paid_clients_current_month(
    organization_paid_macs: set[str] | None = None,
    organization_paid_user_ids: set[str] | None = None,
    organization_paid_vlan_names: set[str] | None = None,
    limit: int = 12,
    period_start: datetime | None = None,
    period_end: datetime | None = None,
) -> list[GlobalTopUser]:
    'Return organization-paid usage totals grouped by client for a period.'
    month_start_dt, month_end_dt, _, _ = _month_period_bounds(period_start, period_end)

    mac_allowlist = {mac.strip().lower() for mac in (organization_paid_macs or set()) if mac.strip()}
    user_allowlist = {user_id.strip().lower() for user_id in (organization_paid_user_ids or set()) if user_id.strip()}
    vlan_allowlist = {vlan.strip().lower() for vlan in (organization_paid_vlan_names or set()) if vlan.strip()}

    stmt = (
        select(UsageRecord.timestamp, UsageRecord.mac, UsageRecord.user_id, UsageRecord.name, UsageRecord.vlan, UsageRecord.mb_used)
        .where(
            UsageRecord.timestamp >= month_start_dt,
            UsageRecord.timestamp <= month_end_dt,
        )
    )

    with SessionLocal() as session:
        rows = session.execute(stmt).all()

    client_totals: dict[str, tuple[float, int]] = {}
    client_latest_identity: dict[str, tuple[datetime, str, str]] = {}
    client_latest_vlan: dict[str, str] = {}

    for row_timestamp, row_mac, row_user_id, row_name, row_vlan, row_mb_used in rows:
        if not isinstance(row_timestamp, datetime) or not isinstance(row_mac, str):
            continue

        mac_value = row_mac.strip().lower() if row_mac.strip() else ''
        user_value = row_user_id.strip().lower() if isinstance(row_user_id, str) and row_user_id.strip() else ''
        vlan_value = row_vlan.strip().lower() if isinstance(row_vlan, str) and row_vlan.strip() else ''
        is_organization_paid = (
            vlan_value in vlan_allowlist
            or mac_value in mac_allowlist
            or user_value in user_allowlist
        )
        if not is_organization_paid:
            continue

        total_mb, active_minutes = client_totals.get(row_mac, (0.0, 0))
        client_totals[row_mac] = (total_mb + float(row_mb_used or 0.0), active_minutes + 1)

        resolved_name = row_name.strip() if isinstance(row_name, str) and row_name.strip() else row_mac
        resolved_user_id = row_user_id.strip() if isinstance(row_user_id, str) and row_user_id.strip() else ''
        previous_identity = client_latest_identity.get(row_mac)
        if previous_identity is None or row_timestamp >= previous_identity[0]:
            client_latest_identity[row_mac] = (row_timestamp, resolved_name, resolved_user_id)
            client_latest_vlan[row_mac] = row_vlan.strip() if isinstance(row_vlan, str) and row_vlan.strip() else ''

    results = sorted(
        (
            GlobalTopUser(
                mac=mac,
                name=client_latest_identity.get(mac, (datetime.min, mac, ''))[1],
                user_id=client_latest_identity.get(mac, (datetime.min, '', ''))[2],
                vlan_name=client_latest_vlan.get(mac, ''),
                total_mb=totals[0],
                active_minutes=totals[1],
            )
            for mac, totals in client_totals.items()
        ),
        key=lambda row: (row.total_mb, row.active_minutes),
        reverse=True,
    )

    return results[:max(1, limit)]


def get_usage_months() -> list[date]:
    'Return calendar months with any usage records, newest first.'
    stmt = select(UsageRecord.timestamp).order_by(UsageRecord.timestamp.desc())

    with SessionLocal() as session:
        rows = session.execute(stmt).all()

    month_starts: set[date] = set()
    for (row_timestamp,) in rows:
        if isinstance(row_timestamp, datetime):
            month_starts.add(date(row_timestamp.year, row_timestamp.month, 1))

    return sorted(month_starts, reverse=True)


def get_global_concurrency_insights_current_month(
    period_start: datetime | None = None,
    period_end: datetime | None = None,
) -> GlobalConcurrencyInsights:
    'Return daily peak concurrency and day/hour heatmap totals for a month period.'
    month_start_dt, month_end_dt, month_start, month_end = _month_period_bounds(period_start, period_end)

    stmt = (
        select(UsageRecord.timestamp, UsageRecord.mac)
        .where(
            UsageRecord.timestamp >= month_start_dt,
            UsageRecord.timestamp <= month_end_dt,
        )
    )
    with SessionLocal() as session:
        rows = session.execute(stmt).all()

    minute_clients: dict[datetime, set[str]] = {}
    for row_timestamp, row_mac in rows:
        if not isinstance(row_timestamp, datetime) or not isinstance(row_mac, str):
            continue
        minute_bucket = row_timestamp.replace(second=0, microsecond=0)
        minute_clients.setdefault(minute_bucket, set()).add(row_mac)

    daily_peak_map: dict[date, tuple[int, datetime | None]] = {}
    heatmap_totals: dict[tuple[int, int], float] = {}
    heatmap_counts: dict[tuple[int, int], int] = {}

    for minute_bucket, macs in minute_clients.items():
        concurrent_count = len(macs)
        usage_day = minute_bucket.date()
        peak_count, peak_minute = daily_peak_map.get(usage_day, (0, None))
        if concurrent_count > peak_count:
            daily_peak_map[usage_day] = (concurrent_count, minute_bucket)

        day_of_week = minute_bucket.weekday()  # Monday=0
        hour = minute_bucket.hour
        cell_key = (day_of_week, hour)
        heatmap_totals[cell_key] = float(heatmap_totals.get(cell_key, 0.0)) + float(concurrent_count)
        heatmap_counts[cell_key] = int(heatmap_counts.get(cell_key, 0)) + 1

    daily_x_labels: list[int] = []
    daily_full_labels: list[str] = []
    daily_peak_counts: list[int] = []
    daily_peak_time_labels: list[str] = []
    day_cursor = month_start
    while day_cursor <= month_end:
        peak_count, peak_minute = daily_peak_map.get(day_cursor, (0, None))
        daily_x_labels.append(day_cursor.day)
        daily_full_labels.append(f'{day_cursor.strftime("%b")} {day_cursor.day}')
        daily_peak_counts.append(int(peak_count))
        daily_peak_time_labels.append(peak_minute.strftime('%H:%M') if peak_minute else '')
        day_cursor += timedelta(days=1)

    heatmap_day_labels = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
    heatmap_hour_labels = [f'{hour:02d}:00' for hour in range(24)]
    heatmap_values: list[list[float]] = []
    heatmap_sample_counts: list[list[int]] = []
    for day_of_week in range(7):
        row: list[float] = []
        count_row: list[int] = []
        for hour in range(24):
            cell_key = (day_of_week, hour)
            total = float(heatmap_totals.get(cell_key, 0.0))
            count = int(heatmap_counts.get(cell_key, 0))
            row.append(total)
            count_row.append(count)
        heatmap_values.append(row)
        heatmap_sample_counts.append(count_row)

    return GlobalConcurrencyInsights(
        daily_x_labels=daily_x_labels,
        daily_full_labels=daily_full_labels,
        daily_peak_counts=daily_peak_counts,
        daily_peak_time_labels=daily_peak_time_labels,
        heatmap_day_labels=heatmap_day_labels,
        heatmap_hour_labels=heatmap_hour_labels,
        heatmap_values=heatmap_values,
        heatmap_sample_counts=heatmap_sample_counts,
    )


def get_global_throttling_effectiveness_current_month(
    before_after_days: int = 7,
    max_events: int = 8,
    period_start: datetime | None = None,
    period_end: datetime | None = None,
) -> GlobalThrottlingEffectiveness:
    'Return throttling profile minutes and before/after impact around profile changes.'
    month_start_dt, month_end_dt, _, _ = _month_period_bounds(period_start, period_end)
    window_days = max(1, before_after_days)
    lookback_start = month_start_dt - timedelta(days=window_days)
    lookback_end = month_end_dt + timedelta(days=window_days)

    stmt = (
        select(
            UsageRecord.timestamp,
            UsageRecord.mac,
            UsageRecord.user_id,
            UsageRecord.name,
            UsageRecord.profile,
            UsageRecord.mb_used,
        )
        .where(
            UsageRecord.timestamp >= lookback_start,
            UsageRecord.timestamp <= lookback_end,
        )
        .order_by(UsageRecord.mac.asc(), UsageRecord.timestamp.asc())
    )
    with SessionLocal() as session:
        rows = session.execute(stmt).all()

    profile_minutes: dict[str, int] = {}
    total_active_minutes = 0
    throttled_minutes = 0

    per_mac_rows: dict[str, list[tuple[datetime, str, float]]] = {}
    change_events_raw: list[tuple[datetime, str, str, str, str, str]] = []
    previous_profile_by_mac: dict[str, str] = {}

    for row_timestamp, row_mac, row_user_id, row_name, row_profile, row_mb_used in rows:
        if not isinstance(row_timestamp, datetime) or not isinstance(row_mac, str):
            continue

        mac = row_mac
        user_id = row_user_id.strip() if isinstance(row_user_id, str) and row_user_id.strip() else ''
        name = row_name.strip() if isinstance(row_name, str) and row_name.strip() else mac
        profile_key = row_profile.strip() if isinstance(row_profile, str) and row_profile.strip() else ''
        mb_used = float(row_mb_used or 0.0)

        per_mac_rows.setdefault(mac, []).append((row_timestamp, profile_key, mb_used))

        if month_start_dt <= row_timestamp <= month_end_dt:
            profile_minutes[profile_key] = int(profile_minutes.get(profile_key, 0)) + 1
            total_active_minutes += 1
            if profile_key:
                throttled_minutes += 1

        previous_profile = previous_profile_by_mac.get(mac)
        if (
            previous_profile is not None
            and previous_profile != profile_key
            and month_start_dt <= row_timestamp <= month_end_dt
            and profile_key
        ):
            previous_profile_key = str(previous_profile)
            change_events_raw.append((row_timestamp, mac, user_id, name, previous_profile_key, profile_key))
        previous_profile_by_mac[mac] = profile_key

    def calculate_window_metrics(
        mac_rows: list[tuple[datetime, str, float]],
        start_at: datetime,
        end_at: datetime,
    ) -> tuple[float, float, int]:
        window_total_mb = 0.0
        window_active_minutes = 0
        window_throttled_minutes = 0
        for row_timestamp, profile_key, mb_used in mac_rows:
            if row_timestamp < start_at or row_timestamp >= end_at:
                continue
            window_total_mb += mb_used
            window_active_minutes += 1
            if profile_key:
                window_throttled_minutes += 1
        window_avg_mb_per_day = window_total_mb / float(window_days)
        window_throttled_pct = (
            (window_throttled_minutes * 100.0 / float(window_active_minutes))
            if window_active_minutes > 0
            else 0.0
        )
        return window_avg_mb_per_day, window_throttled_pct, window_active_minutes

    change_events: list[ThrottlingChangeImpact] = []
    for changed_at, mac, user_id, name, from_profile, to_profile in sorted(change_events_raw, key=lambda row: row[0], reverse=True):
        if len(change_events) >= max(1, max_events):
            break
        mac_rows = per_mac_rows.get(mac, [])
        before_start = changed_at - timedelta(days=window_days)
        before_end = changed_at
        after_start = changed_at
        after_end = changed_at + timedelta(days=window_days)
        before_avg_mb_per_day, before_throttled_pct, before_active_minutes = calculate_window_metrics(mac_rows, before_start, before_end)
        after_avg_mb_per_day, after_throttled_pct, after_active_minutes = calculate_window_metrics(mac_rows, after_start, after_end)
        change_events.append(
            ThrottlingChangeImpact(
                changed_at=changed_at,
                mac=mac,
                user_id=user_id,
                name=name,
                from_profile=from_profile,
                to_profile=to_profile,
                before_avg_mb_per_day=before_avg_mb_per_day,
                after_avg_mb_per_day=after_avg_mb_per_day,
                before_throttled_pct=before_throttled_pct,
                after_throttled_pct=after_throttled_pct,
                before_active_minutes=before_active_minutes,
                after_active_minutes=after_active_minutes,
            )
        )

    throttled_pct = ((throttled_minutes * 100.0) / float(total_active_minutes)) if total_active_minutes > 0 else 0.0
    return GlobalThrottlingEffectiveness(
        profile_minutes=profile_minutes,
        total_active_minutes=total_active_minutes,
        throttled_minutes=throttled_minutes,
        throttled_pct=throttled_pct,
        change_events=change_events,
    )
