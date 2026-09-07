import sys
import unittest
from datetime import datetime
from pathlib import Path
from threading import Lock
from unittest.mock import patch


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

import database as db
import monitor


def voucher_summary(*, used_mb: float, allocation_gb: int = 10) -> db.PlusVoucherUsageSummary:
    voucher = db.PlusVoucherRecord(
        id=7,
        batch_id="batch-123",
        user_id=9123,
        password="pass-9123",
        allocation_gb=allocation_gb,
        generated_at=datetime(2026, 5, 30, 12, 0),
        consumed_at=None,
    )
    return db.PlusVoucherUsageSummary(
        voucher=voucher,
        activated_at=datetime(2026, 5, 31, 12, 0),
        used_mb=used_mb,
        remaining_mb=max(0.0, allocation_gb * 1000 - used_mb),
        used_pct=used_mb / (allocation_gb * 1000) * 100,
    )


class VoucherEnforcementTests(unittest.TestCase):
    def test_exhausted_voucher_is_deleted_then_marked_consumed(self) -> None:
        summary = voucher_summary(used_mb=10_000.0)

        with (
            patch.object(monitor.db, "get_active_plus_voucher_summaries", return_value=[summary]) as get_summaries,
            patch.object(monitor.api, "delete_radius_account_by_name", return_value=(True, "")) as delete_account,
            patch.object(monitor.db, "mark_plus_voucher_consumed", return_value=summary.voucher) as mark_consumed,
        ):
            result = monitor.end_exhausted_plus_vouchers()

        get_summaries.assert_called_once_with(force_refresh=True)
        delete_account.assert_called_once_with("9123")
        mark_consumed.assert_called_once_with(7)
        self.assertEqual(result.ended_count, 1)
        self.assertEqual(result.failure_count, 0)

    def test_unifi_failure_leaves_voucher_active_for_retry(self) -> None:
        summary = voucher_summary(used_mb=12_000.0)

        with (
            patch.object(monitor.db, "get_active_plus_voucher_summaries", return_value=[summary]),
            patch.object(monitor.api, "delete_radius_account_by_name", return_value=(False, "controller unavailable")),
            patch.object(monitor.db, "mark_plus_voucher_consumed") as mark_consumed,
        ):
            result = monitor.end_exhausted_plus_vouchers()

        mark_consumed.assert_not_called()
        self.assertEqual(result.exhausted_count, 1)
        self.assertEqual(result.ended_count, 0)
        self.assertEqual(result.failure_count, 1)

    def test_voucher_below_allocation_is_not_ended(self) -> None:
        summary = voucher_summary(used_mb=9_999.99)

        with (
            patch.object(monitor.db, "get_active_plus_voucher_summaries", return_value=[summary]),
            patch.object(monitor.api, "delete_radius_account_by_name") as delete_account,
            patch.object(monitor.db, "mark_plus_voucher_consumed") as mark_consumed,
        ):
            result = monitor.end_exhausted_plus_vouchers()

        delete_account.assert_not_called()
        mark_consumed.assert_not_called()
        self.assertEqual(result.exhausted_count, 0)

    def test_successful_flow_import_triggers_voucher_enforcement(self) -> None:
        usage_monitor = object.__new__(monitor.UsageMonitor)
        usage_monitor._flow_import_lock = Lock()
        enforcement_result = monitor.VoucherEnforcementSummary(
            checked_count=2,
            exhausted_count=1,
            ended_count=1,
            failure_count=0,
        )

        with (
            patch.object(monitor.cfg, "FLOW_IMPORT_ENABLED", True),
            patch.object(monitor.cfg, "PLUS_VOUCHER_AUTO_END_ENABLED", True),
            patch.object(monitor, "parse_internal_networks", return_value=[object()]),
            patch.object(monitor, "import_completed_captures", return_value=(1, 5, 0)),
            patch.object(monitor, "notify_dashboard_data_changed"),
            patch.object(monitor, "end_exhausted_plus_vouchers", return_value=enforcement_result) as enforce,
        ):
            result = usage_monitor._import_flows_now("test")

        self.assertEqual(result, (1, 5, 0))
        enforce.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
