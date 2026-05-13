import sys
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from display_format import format_internet_data_amount, format_voucher_data_amount, format_voucher_percent


class DisplayFormatTests(unittest.TestCase):
    def test_internet_data_amount_shows_small_usage_in_kb(self) -> None:
        self.assertEqual(format_internet_data_amount(0.0), "0 KB")
        self.assertEqual(format_internet_data_amount(0.0004), "<1 KB")
        self.assertEqual(format_internet_data_amount(0.2), "200 KB")
        self.assertEqual(format_internet_data_amount(1.8), "1.8 MB")
        self.assertEqual(format_internet_data_amount(138.0), "138 MB")
        self.assertEqual(format_internet_data_amount(2_207.0), "2.2 GB")

    def test_voucher_data_amount_shows_small_usage_in_mb(self) -> None:
        self.assertEqual(format_voucher_data_amount(0.0), "0 MB")
        self.assertEqual(format_voucher_data_amount(25.0), "25 MB")
        self.assertEqual(format_voucher_data_amount(39_975.0), "39.98 GB")
        self.assertEqual(format_voucher_data_amount(40_000.0), "40 GB")
        self.assertEqual(format_voucher_data_amount(1784.0), "1.8 GB")

    def test_voucher_percent_shows_tiny_nonzero_values(self) -> None:
        self.assertEqual(format_voucher_percent(0.0), "0%")
        self.assertEqual(format_voucher_percent(0.025), "<0.1%")
        self.assertEqual(format_voucher_percent(0.0625), "0.1%")
        self.assertEqual(format_voucher_percent(53.0), "53%")


if __name__ == "__main__":
    unittest.main()
