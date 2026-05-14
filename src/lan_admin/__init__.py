'''LAN management application package.

The current LAN Flask app still lives in the legacy top-level ``app`` module.
New LAN modules should be added under this package as the app is migrated.
'''

from app import create_app

__all__ = ["create_app"]
