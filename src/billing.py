'''Shared billing and usage-cost helpers.'''

import config as cfg


def calculate_month_cost_cents(calendar_month_total_mb: float) -> float:
    'Return month cost in cents using configured rate.'
    return (calendar_month_total_mb / 1000.0) * float(cfg.COST_IN_CENTS_PER_GB)
