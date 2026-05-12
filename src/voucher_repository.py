'''Persistence helpers for generated Plus vouchers.'''

import secrets
from bisect import bisect_right
from datetime import datetime, timedelta
from threading import Lock
import time as monotonic_time

from sqlalchemy import func, select

import database as db


ACTIVE_VOUCHER_SUMMARIES_CACHE_SECONDS = 30.0
_active_voucher_summaries_cache_lock = Lock()
_active_voucher_summaries_cache: tuple[object, float, list[db.PlusVoucherUsageSummary]] | None = None


def _clear_active_voucher_summaries_cache() -> None:
    'Clear cached admin voucher balances after voucher writes.'
    global _active_voucher_summaries_cache
    with _active_voucher_summaries_cache_lock:
        _active_voucher_summaries_cache = None


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
        _clear_active_voucher_summaries_cache()
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


def get_plus_voucher_usage_summary(voucher: db.PlusVoucherRecord) -> tuple[datetime | None, float]:
    'Return WAN-attributed first usage time and lifetime usage for one voucher.'
    return db.get_wan_usage_summary_for_user_id(
        voucher.user_id,
        period_start=voucher.generated_at,
    )


def _build_voucher_summary(
    voucher: db.PlusVoucherRecord,
    activated_at: datetime | None,
    used_mb: float,
) -> db.PlusVoucherUsageSummary:
    'Return the admin usage summary for one voucher.'
    allocation_mb = float(voucher.allocation_gb * 1000)
    remaining_mb = max(0.0, allocation_mb - used_mb)
    used_pct = (used_mb / allocation_mb * 100.0) if allocation_mb else 0.0
    return db.PlusVoucherUsageSummary(
        voucher=voucher,
        activated_at=activated_at,
        used_mb=used_mb,
        remaining_mb=remaining_mb,
        used_pct=used_pct,
    )


def _resolve_flow_identity(
    started_at: datetime,
    identities: list[db.ClientIpIdentityRecord],
    observed_times: list[datetime],
    identity_after_tolerance: timedelta,
) -> db.ClientIpIdentityRecord | None:
    'Return the identity observed closest to one flow timestamp.'
    if not identities or not observed_times:
        return None

    prior_index = bisect_right(observed_times, started_at) - 1
    if prior_index >= 0:
        return identities[prior_index]
    if observed_times[0] <= started_at + identity_after_tolerance:
        return identities[0]
    return None


def _get_plus_voucher_wan_usage_summaries(
    vouchers: list[db.PlusVoucherRecord],
    period_end: datetime | None = None,
    identity_after_tolerance: timedelta = timedelta(minutes=10),
) -> dict[int, tuple[datetime | None, float]]:
    'Return WAN-attributed usage summaries for many vouchers in one candidate-IP flow pass.'
    if not vouchers:
        return {}

    generated_at_by_user_id = {str(voucher.user_id): voucher.generated_at for voucher in vouchers}
    voucher_id_by_user_id = {str(voucher.user_id): voucher.id for voucher in vouchers}
    period_start = min(voucher.generated_at for voucher in vouchers)
    resolved_period_end = period_end or datetime.now()

    candidate_identity_stmt = (
        select(db.ClientIpIdentity.ip_address)
        .where(
            db.ClientIpIdentity.user_id.in_(sorted(generated_at_by_user_id)),
            db.ClientIpIdentity.observed_at >= period_start - timedelta(days=1),
            db.ClientIpIdentity.observed_at <= resolved_period_end + identity_after_tolerance,
        )
    )
    with db.SessionLocal() as session:
        candidate_ips = sorted(
            {
                str(ip_address)
                for ip_address in session.execute(candidate_identity_stmt).scalars()
                if ip_address
            }
        )

    if not candidate_ips:
        return {}

    identity_stmt = (
        select(db.ClientIpIdentity)
        .where(
            db.ClientIpIdentity.ip_address.in_(candidate_ips),
            db.ClientIpIdentity.observed_at >= period_start - timedelta(days=1),
            db.ClientIpIdentity.observed_at <= resolved_period_end + identity_after_tolerance,
        )
        .order_by(db.ClientIpIdentity.ip_address.asc(), db.ClientIpIdentity.observed_at.asc(), db.ClientIpIdentity.id.asc())
    )

    identities_by_ip: dict[str, list[db.ClientIpIdentityRecord]] = {client_ip: [] for client_ip in candidate_ips}
    with db.SessionLocal() as session:
        for row in session.execute(identity_stmt).scalars():
            identities_by_ip.setdefault(row.ip_address, []).append(
                db.ClientIpIdentityRecord(
                    observed_at=row.observed_at,
                    ip_address=row.ip_address,
                    mac=row.mac,
                    name=row.name,
                    user_id=row.user_id,
                    vlan=row.vlan,
                )
            )

    flow_stmt = (
        select(
            db.WanFlowUsage.started_at,
            db.WanFlowUsage.client_ip,
            db.WanFlowUsage.bytes,
        )
        .where(
            db.WanFlowUsage.client_ip.in_(candidate_ips),
            db.WanFlowUsage.started_at >= period_start,
            db.WanFlowUsage.started_at <= resolved_period_end,
        )
        .order_by(db.WanFlowUsage.started_at.asc(), db.WanFlowUsage.id.asc())
    )
    with db.SessionLocal() as session:
        flow_rows = session.execute(flow_stmt).all()

    observed_times_by_ip = {
        client_ip: [identity.observed_at for identity in identities]
        for client_ip, identities in identities_by_ip.items()
    }
    first_usage_at_by_voucher_id: dict[int, datetime] = {}
    total_bytes_by_voucher_id: dict[int, int] = {}
    for started_at, client_ip, byte_count in flow_rows:
        ip_text = str(client_ip)
        identity = _resolve_flow_identity(
            started_at,
            identities_by_ip.get(ip_text, []),
            observed_times_by_ip.get(ip_text, []),
            identity_after_tolerance,
        )
        if identity is None:
            continue

        user_id = identity.user_id.strip()
        voucher_id = voucher_id_by_user_id.get(user_id)
        generated_at = generated_at_by_user_id.get(user_id)
        if voucher_id is None or generated_at is None or started_at < generated_at:
            continue

        first_usage_at_by_voucher_id.setdefault(voucher_id, started_at)
        total_bytes_by_voucher_id[voucher_id] = total_bytes_by_voucher_id.get(voucher_id, 0) + int(byte_count or 0)

    return {
        voucher_id: (
            first_usage_at_by_voucher_id.get(voucher_id),
            total_bytes / 1_000_000.0,
        )
        for voucher_id, total_bytes in total_bytes_by_voucher_id.items()
    }


def get_active_plus_voucher_summaries() -> list[db.PlusVoucherUsageSummary]:
    'Return active voucher balances for admin review.'
    global _active_voucher_summaries_cache
    cache_key = db.SessionLocal
    now_monotonic = monotonic_time.monotonic()
    with _active_voucher_summaries_cache_lock:
        if (
            _active_voucher_summaries_cache
            and _active_voucher_summaries_cache[0] is cache_key
            and _active_voucher_summaries_cache[1] > now_monotonic
        ):
            return list(_active_voucher_summaries_cache[2])

    stmt = (
        select(db.PlusVoucher)
        .where(db.PlusVoucher.consumed_at.is_(None))
        .order_by(db.PlusVoucher.generated_at.desc(), db.PlusVoucher.id.desc())
    )
    with db.SessionLocal() as session:
        vouchers = [_voucher_record(row) for row in session.execute(stmt).scalars().all()]

    wan_summaries = _get_plus_voucher_wan_usage_summaries(vouchers)
    summaries: list[db.PlusVoucherUsageSummary] = []
    for voucher in vouchers:
        wan_activated_at, wan_used_mb = wan_summaries.get(voucher.id, (None, 0.0))
        summaries.append(_build_voucher_summary(voucher, wan_activated_at, wan_used_mb))

    summaries.sort(
        key=lambda summary: (
            summary.activated_at or summary.voucher.generated_at,
            summary.voucher.generated_at,
            summary.voucher.id,
        ),
        reverse=True,
    )
    summaries.sort(key=lambda summary: summary.activated_at is None)
    with _active_voucher_summaries_cache_lock:
        _active_voucher_summaries_cache = (
            cache_key,
            monotonic_time.monotonic() + ACTIVE_VOUCHER_SUMMARIES_CACHE_SECONDS,
            list(summaries),
        )
    return summaries
