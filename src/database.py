import sqlite3
from dataclasses import dataclass
from datetime import datetime, time, timedelta
from pathlib import Path
from typing import Optional
from sqlalchemy import create_engine, String, func, select, event
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker

from clientinfo import ClientInfo

# --- SETUP & CONCURRENCY ---
DB_PATH = Path(__file__).resolve().parent.parent / "meter.db"
DB_URL = f"sqlite:///{DB_PATH}"
engine = create_engine(DB_URL, echo=False)

@event.listens_for(engine, "connect")
def set_sqlite_pragma(dbapi_connection, _connection_record):
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
    pass


@dataclass(frozen=True, kw_only=True)
class DailyUsageSummary:
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

# --- MODELS ---
class UsageRecord(Base):
    """The Ledger: Stores every non-zero minute of usage."""
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
def init_db():
    Base.metadata.create_all(bind=engine)

def log_usage(c: ClientInfo, interval_mb):
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


def get_daily_usage_summary() -> list[DailyUsageSummary]:
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
    stmt = (
        select(UsageRecord)
        .where(UsageRecord.mac == mac)
        .order_by(UsageRecord.timestamp.desc())
        .limit(limit)
    )

    with SessionLocal() as session:
        return session.execute(stmt).scalars().all()
