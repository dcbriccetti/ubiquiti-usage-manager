'''Persistence helpers for generated Plus vouchers.'''

import secrets

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
