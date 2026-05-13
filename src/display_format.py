'''Shared display-format helpers for templates.'''


def format_voucher_data_amount(mb_value: float) -> str:
    'Render voucher data amounts without hiding small nonzero usage.'
    if mb_value == 0:
        return '0 MB'
    if abs(mb_value) < 1000:
        return f'{mb_value:,.0f} MB'
    return f'{mb_value / 1000.0:,.1f} GB'


def format_voucher_percent(percent_value: float) -> str:
    'Render voucher percent used without rounding tiny nonzero values to 0%.'
    if percent_value == 0:
        return '0%'
    if round(abs(percent_value), 1) == 0:
        return '<0.1%'
    if abs(percent_value) < 10:
        return f'{percent_value:,.1f}%'
    return f'{percent_value:,.0f}%'
