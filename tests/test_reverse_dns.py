import sys
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from reverse_dns import resolve_host_labels, safe_hostname_label, safe_ip_label, shorten_hostname


class ReverseDnsTests(unittest.TestCase):
    def test_shorten_hostname_keeps_short_names(self) -> None:
        self.assertEqual(shorten_hostname("example.com"), "example.com")
        self.assertEqual(shorten_hostname("f14.1e100.net."), "f14.1e100.net")

    def test_shorten_hostname_compacts_long_cdn_names(self) -> None:
        self.assertEqual(
            shorten_hostname("a184-28-149-222.deploy.static.akamaitechnologies.com"),
            "static.akamaitechnologies.com",
        )
        self.assertEqual(
            shorten_hostname("server-108-139-10-20.sfo5.r.cloudfront.net"),
            "r.cloudfront.net",
        )
        self.assertEqual(
            shorten_hostname("edge.cache.verylonghostnamepart.example.net"),
            "example.net",
        )

    def test_safe_hostname_label_hides_raw_hostnames(self) -> None:
        self.assertEqual(
            safe_hostname_label("a184-28-149-222.deploy.static.akamaitechnologies.com"),
            "Akamai CDN host",
        )
        self.assertEqual(
            safe_hostname_label("server-108-139-10-20.sfo5.r.cloudfront.net"),
            "Amazon CDN host",
        )
        self.assertEqual(
            safe_hostname_label("ec2-203-0-113-10.compute-1.amazonaws.com"),
            "Amazon cloud host",
        )
        self.assertEqual(
            safe_hostname_label("potentially-sensitive.example.net"),
            None,
        )

    def test_safe_ip_label_marks_known_public_provider_ranges(self) -> None:
        self.assertEqual(safe_ip_label("17.248.192.26"), "Apple host")
        self.assertEqual(safe_ip_label("1.1.1.1"), "Cloudflare DNS")
        self.assertEqual(safe_ip_label("1.0.0.1"), "Cloudflare DNS")
        self.assertEqual(safe_ip_label("8.8.8.8"), "Google DNS")
        self.assertEqual(safe_ip_label("8.8.4.4"), "Google DNS")
        self.assertEqual(safe_ip_label("9.9.9.9"), "Quad9 DNS")
        self.assertEqual(safe_ip_label("149.112.112.112"), "Quad9 DNS")
        self.assertEqual(safe_ip_label("208.67.222.222"), "Cisco DNS")
        self.assertEqual(safe_ip_label("208.67.220.220"), "Cisco DNS")
        self.assertEqual(safe_ip_label("208.67.219.220"), None)

    def test_resolve_host_labels_uses_safe_ip_labels_without_dns(self) -> None:
        self.assertEqual(
            resolve_host_labels(["17.248.192.26", "8.8.8.8"], wait=False),
            {"17.248.192.26": "Apple host", "8.8.8.8": "Google DNS"},
        )


if __name__ == "__main__":
    unittest.main()
