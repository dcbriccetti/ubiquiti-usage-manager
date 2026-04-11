from datetime import datetime, time
from typing import Optional
from sqlalchemy import create_engine, String, func, select, event
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker

from clientinfo import ClientInfo

# --- SETUP & CONCURRENCY ---
DB_URL = "sqlite:///meter.db"
engine = create_engine(DB_URL, echo=False)

@event.listens_for(engine, "connect")
def set_sqlite_pragma(dbapi_connection, _connection_record):
    cursor = dbapi_connection.cursor()
    cursor.execute("PRAGMA journal_mode=WAL")
    cursor.execute("PRAGMA synchronous=NORMAL")
    cursor.close()

SessionLocal = sessionmaker(bind=engine)

class Base(DeclarativeBase):
    pass

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
    if round(c.mb_used_since_connection, 3) <= 0:
        return

    with SessionLocal() as session:
        record = UsageRecord(
            user_id=c.user_id,
            mac=c.mac,
            name=c.name,
            vlan=c.vlan_name,
            mb_used=c.mb_used_since_connection,
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
