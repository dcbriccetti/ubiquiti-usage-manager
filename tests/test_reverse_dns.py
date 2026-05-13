import sys
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from reverse_dns import safe_hostname_label, shorten_hostname


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
            "Named Internet host",
        )


if __name__ == "__main__":
    unittest.main()
