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
