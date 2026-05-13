'''Shared display-format helpers for templates.'''


def format_voucher_data_amount(mb_value: float) -> str:
    'Render voucher data amounts without hiding small nonzero usage.'
    if mb_value == 0:
        return '0 MB'
    if abs(mb_value) < 1000:
        return f'{mb_value:,.0f} MB'

    gb_value = mb_value / 1000.0
    if mb_value % 1000 == 0:
        return f'{gb_value:,.0f} GB'
    if abs(gb_value) >= 10:
        return f'{gb_value:,.2f} GB'
    return f'{gb_value:,.1f} GB'


def format_internet_data_amount(mb_value: float) -> str:
    'Render Internet data amounts without rounding small nonzero activity to zero.'
    if mb_value == 0:
        return '0 KB'

    if abs(mb_value) < 1:
        kb_value = mb_value * 1000.0
        if round(abs(kb_value)) == 0:
            return '<1 KB'
        return f'{kb_value:,.0f} KB'

    if abs(mb_value) < 10:
        return f'{mb_value:,.1f} MB'
    if abs(mb_value) < 1000:
        return f'{mb_value:,.0f} MB'

    gb_value = mb_value / 1000.0
    if abs(gb_value) < 10:
        return f'{gb_value:,.1f} GB'
    return f'{gb_value:,.0f} GB'


def format_voucher_percent(percent_value: float) -> str:
    'Render voucher percent used without rounding tiny nonzero values to 0%.'
    if percent_value == 0:
        return '0%'
    if round(abs(percent_value), 1) == 0:
        return '<0.1%'
    if abs(percent_value) < 10:
        return f'{percent_value:,.1f}%'
    return f'{percent_value:,.0f}%'
