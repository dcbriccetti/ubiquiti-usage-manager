import sqlite3
from dataclasses import dataclass
from datetime import datetime, time, timedelta
from pathlib import Path
from typing import Optional, cast
from sqlalchemy import create_engine, String, func, select, event
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker

from clientinfo import ClientInfo

# --- SETUP & CONCURRENCY ---
DB_PATH = Path(__file__).resolve().parent.parent / "meter.db"
DB_URL = f"sqlite:///{DB_PATH}"
engine = create_engine(DB_URL, echo=False)

@event.listens_for(engine, "connect")
def set_sqlite_pragma(dbapi_connection, _connection_record):
    'Apply SQLite pragmas for better concurrent read/write behavior.'
    cursor = dbapi_connection.cursor()
    try:
        # If another process has the DB locked momentarily, continue with defaults.
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.execute("PRAGMA synchronous=NORMAL")
    except sqlite3.OperationalError as exc:
        print(f"⚠️ SQLite PRAGMA setup skipped: {exc}")
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


class MonitorHeartbeat(Base):
    'Single-row heartbeat record updated by the monitor loop.'
    __tablename__ = "monitor_heartbeat"

    id:         Mapped[int]      = mapped_column(primary_key=True)
    updated_at: Mapped[datetime] = mapped_column(default=datetime.now, index=True)

# --- DATABASE API ---
def init_db():
    'Create database tables if they do not already exist.'
    Base.metadata.create_all(bind=engine)

def log_usage(c: ClientInfo, interval_mb):
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


def update_monitor_heartbeat(at: datetime | None = None) -> None:
    'Upsert the monitor heartbeat timestamp.'
    heartbeat_time = at or datetime.now()

    with SessionLocal() as session:
        heartbeat = session.get(MonitorHeartbeat, 1)
        if heartbeat is None:
            heartbeat = MonitorHeartbeat(id=1, updated_at=heartbeat_time)
            session.add(heartbeat)
        else:
            heartbeat.updated_at = heartbeat_time
        session.commit()


def get_monitor_heartbeat() -> datetime | None:
    'Return the last monitor heartbeat time, if available.'
    with SessionLocal() as session:
        heartbeat: MonitorHeartbeat | None = session.get(MonitorHeartbeat, 1)
        if heartbeat is None:
            return None
        return cast(datetime, heartbeat.updated_at)
