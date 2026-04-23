'Helpers for parsing and normalizing UniFi time-like fields.'

from datetime import datetime


def parse_non_negative_int(value: object) -> int | None:
    'Return int when value is parseable and >= 0, otherwise None.'
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value if value >= 0 else None
    if isinstance(value, float):
        return int(value) if value >= 0 else None
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            parsed = int(float(text))
        except ValueError:
            return None
        return parsed if parsed >= 0 else None
    return None


def normalize_online_seconds(raw_value: object, now_epoch_seconds: int | None = None) -> int | None:
    'Normalize UniFi online duration input into elapsed seconds.'
    parsed = parse_non_negative_int(raw_value)
    if parsed is None:
        return None

    # UniFi may report epoch timestamps in milliseconds.
    if parsed > 10_000_000_000:
        parsed = parsed // 1000

    if now_epoch_seconds is None:
        now_epoch_seconds = int(datetime.now().timestamp())

    # Treat plausible epoch timestamps as "connected since".
    if 946684800 <= parsed <= now_epoch_seconds + 86400:
        return max(0, now_epoch_seconds - parsed)
    return parsed


def normalize_epoch_seconds(raw_value: object) -> int | None:
    'Normalize UniFi epoch-ish timestamp values into epoch seconds.'
    parsed = parse_non_negative_int(raw_value)
    if parsed is None:
        return None

    if parsed > 10_000_000_000:
        parsed = parsed // 1000
    return parsed
