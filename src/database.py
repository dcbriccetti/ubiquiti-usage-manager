'''Database access layer for usage metering and dashboard aggregation.

This module owns SQLite/SQLAlchemy setup and all persisted usage queries so
monitoring/runtime code does not need direct SQL concerns.
'''

import sqlite3
import logging
from dataclasses import dataclass
from datetime import datetime, date, time, timedelta
from pathlib import Path
from typing import Optional, cast
from sqlalchemy import create_engine, String, func, select, event
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


# --- DATABASE API ---
def init_db() -> None:
    'Create database tables if they do not already exist.'
    Base.metadata.create_all(bind=engine)

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


def get_global_month_insights(top_limit: int = 5) -> GlobalInsights:
    'Return month-to-date global analytics for dashboard insights panels.'
    now = datetime.now()
    month_start = date(now.year, now.month, 1)
    today = now.date()
    month_start_dt = datetime.combine(month_start, time.min)
    month_end_dt = datetime.combine(today, time.max)

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
    while day <= today:
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
        key=lambda row: (row.active_minutes, row.total_mb),
        reverse=True,
    )[:max(1, top_limit)]

    return GlobalInsights(
        active_users_min=active_users_min,
        active_users_mean=active_users_mean,
        active_users_max=active_users_max,
        active_users_today=active_users_today,
        days_in_period=len(daily_counts),
        active_users_daily_x_labels=[day_number for day_number in range(1, len(daily_counts) + 1)],
        active_users_daily_full_labels=[f'{month_start.strftime("%b")} {day_number}' for day_number in range(1, len(daily_counts) + 1)],
        active_users_daily_counts=daily_counts,
        top_users=top_users,
        top_access_points=top_access_points,
    )


def get_global_daily_network_usage_current_month() -> list[GlobalDailyNetworkUsage]:
    'Return daily Basic/Plus usage totals (MB + active minutes) for current month.'
    now = datetime.now()
    month_start = date(now.year, now.month, 1)
    today = now.date()
    month_start_dt = datetime.combine(month_start, time.min)
    month_end_dt = datetime.combine(today, time.max)

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

    totals_by_day: dict[date, dict[str, float | int]] = {}
    for row_timestamp, row_vlan, row_mb_used in rows:
        if not isinstance(row_timestamp, datetime):
            continue

        usage_day = row_timestamp.date()
        bucket = totals_by_day.setdefault(
            usage_day,
            {
                'basic_mb': 0.0,
                'plus_mb': 0.0,
                'basic_minutes': 0,
                'plus_minutes': 0,
            },
        )

        vlan_label = row_vlan.strip().lower() if isinstance(row_vlan, str) and row_vlan.strip() else ''
        mb_used = float(row_mb_used or 0.0)
        if vlan_label == 'plus':
            bucket['plus_mb'] = float(bucket['plus_mb']) + mb_used
            bucket['plus_minutes'] = int(bucket['plus_minutes']) + 1
        else:
            bucket['basic_mb'] = float(bucket['basic_mb']) + mb_used
            bucket['basic_minutes'] = int(bucket['basic_minutes']) + 1

    day = month_start
    series: list[GlobalDailyNetworkUsage] = []
    while day <= today:
        bucket = totals_by_day.get(day, {})
        series.append(
            GlobalDailyNetworkUsage(
                usage_day=day,
                basic_mb=float(bucket.get('basic_mb', 0.0)),
                plus_mb=float(bucket.get('plus_mb', 0.0)),
                basic_minutes=int(bucket.get('basic_minutes', 0)),
                plus_minutes=int(bucket.get('plus_minutes', 0)),
            )
        )
        day += timedelta(days=1)

    return series


def get_global_payer_split_current_month(
    organization_paid_macs: set[str] | None = None,
    organization_paid_user_ids: set[str] | None = None,
    organization_paid_vlan_names: set[str] | None = None,
) -> GlobalPayerSplit:
    'Return month-to-date totals split into organization-paid vs user-paid activity.'
    now = datetime.now()
    month_start = date(now.year, now.month, 1)
    today = now.date()
    month_start_dt = datetime.combine(month_start, time.min)
    month_end_dt = datetime.combine(today, time.max)

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
) -> list[GlobalTopUser]:
    'Return top month-to-date users by MB, with optional organization-paid exclusion.'
    now = datetime.now()
    month_start = date(now.year, now.month, 1)
    today = now.date()
    month_start_dt = datetime.combine(month_start, time.min)
    month_end_dt = datetime.combine(today, time.max)

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
) -> list[GlobalTopUser]:
    'Return month-to-date organization-paid usage totals grouped by client.'
    now = datetime.now()
    month_start = date(now.year, now.month, 1)
    today = now.date()
    month_start_dt = datetime.combine(month_start, time.min)
    month_end_dt = datetime.combine(today, time.max)

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


def get_global_concurrency_insights_current_month() -> GlobalConcurrencyInsights:
    'Return daily peak concurrency and day/hour heatmap averages for current month.'
    now = datetime.now()
    month_start = date(now.year, now.month, 1)
    today = now.date()
    month_start_dt = datetime.combine(month_start, time.min)
    month_end_dt = datetime.combine(today, time.max)

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
    while day_cursor <= today:
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
            row.append((total / count) if count > 0 else 0.0)
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
) -> GlobalThrottlingEffectiveness:
    'Return throttling profile minutes and before/after impact around profile changes.'
    now = datetime.now()
    month_start = date(now.year, now.month, 1)
    month_start_dt = datetime.combine(month_start, time.min)
    window_days = max(1, before_after_days)
    lookback_start = now - timedelta(days=(window_days * 3))

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
            UsageRecord.timestamp <= now,
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

        if row_timestamp >= month_start_dt:
            profile_minutes[profile_key] = int(profile_minutes.get(profile_key, 0)) + 1
            total_active_minutes += 1
            if profile_key:
                throttled_minutes += 1

        previous_profile = previous_profile_by_mac.get(mac)
        if (
            previous_profile is not None
            and previous_profile != profile_key
            and row_timestamp >= month_start_dt
            and profile_key
        ):
            change_events_raw.append((row_timestamp, mac, user_id, name, previous_profile, profile_key))
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
