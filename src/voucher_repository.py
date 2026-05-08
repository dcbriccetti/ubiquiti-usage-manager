'''Persistence helpers for generated Plus vouchers.'''

import secrets
from datetime import datetime

from sqlalchemy import func, select

import database as db


def _voucher_record(row: db.PlusVoucher) -> db.PlusVoucherRecord:
    'Return an immutable voucher view-model from an ORM row.'
    return db.PlusVoucherRecord(
        id=row.id,
        batch_id=row.batch_id,
        user_id=row.user_id,
        password=row.password,
        allocation_gb=row.allocation_gb,
        generated_at=row.generated_at,
        consumed_at=row.consumed_at,
    )


def _generate_voucher_password(length: int = 8) -> str:
    'Return a readable random password for a paper voucher.'
    alphabet = 'abcdefghjkmnpqrstuvwxyz23456789'
    return ''.join(secrets.choice(alphabet) for _ in range(length))


def create_plus_vouchers(count: int, allocation_gb: int) -> list[db.PlusVoucherRecord]:
    'Create unconsumed Plus vouchers with unique active integer user IDs.'
    if count < 1:
        raise ValueError('Voucher count must be at least 1.')
    if allocation_gb < 1:
        raise ValueError('Voucher allocation must be at least 1 GB.')

    with db.SessionLocal() as session:
        batch_id = secrets.token_hex(8)
        active_user_ids = {
            int(user_id)
            for user_id in session.execute(
                select(db.PlusVoucher.user_id).where(db.PlusVoucher.consumed_at.is_(None))
            ).scalars()
        }
        available_user_ids = [user_id for user_id in range(1, 10_000) if user_id not in active_user_ids]
        if count > len(available_user_ids):
            raise ValueError(f'Only {len(available_user_ids)} unconsumed voucher user IDs are available.')

        selected_user_ids = secrets.SystemRandom().sample(available_user_ids, count)
        vouchers = [
            db.PlusVoucher(
                batch_id=batch_id,
                user_id=user_id,
                password=_generate_voucher_password(),
                allocation_gb=allocation_gb,
            )
            for user_id in selected_user_ids
        ]
        session.add_all(vouchers)
        session.commit()
        vouchers.sort(key=lambda voucher: voucher.user_id)
        return [_voucher_record(voucher) for voucher in vouchers]


def get_plus_voucher_batch(batch_id: str) -> list[db.PlusVoucherRecord]:
    'Return all vouchers for one generated batch, ordered for printing.'
    stmt = (
        select(db.PlusVoucher)
        .where(db.PlusVoucher.batch_id == batch_id)
        .order_by(db.PlusVoucher.user_id.asc())
    )
    with db.SessionLocal() as session:
        rows = session.execute(stmt).scalars().all()
        return [_voucher_record(row) for row in rows]


def get_plus_vouchers(limit: int = 200) -> list[db.PlusVoucherRecord]:
    'Return recent Plus vouchers, newest first.'
    stmt = (
        select(db.PlusVoucher)
        .order_by(db.PlusVoucher.generated_at.desc(), db.PlusVoucher.id.desc())
        .limit(max(1, limit))
    )
    with db.SessionLocal() as session:
        rows = session.execute(stmt).scalars().all()
        return [_voucher_record(row) for row in rows]


def get_unconsumed_plus_voucher_count() -> int:
    'Return the number of generated vouchers that have not been consumed.'
    stmt = select(func.count()).select_from(db.PlusVoucher).where(db.PlusVoucher.consumed_at.is_(None))
    with db.SessionLocal() as session:
        return int(session.execute(stmt).scalar() or 0)


def get_active_plus_voucher_for_user_id(user_id: str | int | None) -> db.PlusVoucherRecord | None:
    'Return the unconsumed voucher matching one RADIUS user ID.'
    if user_id is None:
        return None

    try:
        voucher_user_id = int(str(user_id).strip())
    except ValueError:
        return None

    stmt = (
        select(db.PlusVoucher)
        .where(
            db.PlusVoucher.user_id == voucher_user_id,
            db.PlusVoucher.consumed_at.is_(None),
        )
        .order_by(db.PlusVoucher.generated_at.desc(), db.PlusVoucher.id.desc())
        .limit(1)
    )
    with db.SessionLocal() as session:
        row = session.execute(stmt).scalar_one_or_none()
        return _voucher_record(row) if row else None


def get_plus_voucher_legacy_usage_summary(voucher: db.PlusVoucherRecord) -> tuple[datetime | None, float]:
    'Return first sampled UniFi usage time and lifetime usage for one voucher.'
    user_id = str(voucher.user_id)
    first_usage_stmt = select(func.min(db.UsageRecord.timestamp)).where(
        db.UsageRecord.user_id == user_id,
        db.UsageRecord.timestamp >= voucher.generated_at,
    )
    with db.SessionLocal() as session:
        first_usage_at = session.execute(first_usage_stmt).scalar()
        if first_usage_at is None:
            return None, 0.0

        total_usage_stmt = select(func.sum(db.UsageRecord.mb_used)).where(
            db.UsageRecord.user_id == user_id,
            db.UsageRecord.timestamp >= first_usage_at,
        )
        return first_usage_at, float(session.execute(total_usage_stmt).scalar() or 0.0)


def get_plus_voucher_usage_summary(voucher: db.PlusVoucherRecord) -> tuple[datetime | None, float]:
    'Return first usage time and lifetime usage for one voucher, preferring WAN flow usage.'
    legacy_activated_at, legacy_used_mb = get_plus_voucher_legacy_usage_summary(voucher)
    wan_activated_at, wan_used_mb = db.get_wan_usage_summary_for_user_id(
        voucher.user_id,
        period_start=voucher.generated_at,
    )
    first_wan_flow_at = db.get_first_wan_flow_time()
    if (
        wan_activated_at is not None
        and (
            legacy_activated_at is None
            or first_wan_flow_at is None
            or legacy_activated_at >= first_wan_flow_at
        )
    ):
        return wan_activated_at, wan_used_mb

    return legacy_activated_at, legacy_used_mb


def get_active_plus_voucher_summaries() -> list[db.PlusVoucherUsageSummary]:
    'Return active voucher balances for admin review.'
    stmt = (
        select(db.PlusVoucher)
        .where(db.PlusVoucher.consumed_at.is_(None))
        .order_by(db.PlusVoucher.generated_at.desc(), db.PlusVoucher.id.desc())
    )
    with db.SessionLocal() as session:
        vouchers = [_voucher_record(row) for row in session.execute(stmt).scalars().all()]

    summaries: list[db.PlusVoucherUsageSummary] = []
    for voucher in vouchers:
        activated_at, used_mb = get_plus_voucher_usage_summary(voucher)
        allocation_mb = float(voucher.allocation_gb * 1000)
        remaining_mb = max(0.0, allocation_mb - used_mb)
        used_pct = (used_mb / allocation_mb * 100.0) if allocation_mb else 0.0
        summaries.append(
            db.PlusVoucherUsageSummary(
                voucher=voucher,
                activated_at=activated_at,
                used_mb=used_mb,
                remaining_mb=remaining_mb,
                used_pct=used_pct,
            )
        )

    summaries.sort(
        key=lambda summary: (
            summary.activated_at or summary.voucher.generated_at,
            summary.voucher.generated_at,
            summary.voucher.id,
        ),
        reverse=True,
    )
    summaries.sort(key=lambda summary: summary.activated_at is None)
    return summaries
