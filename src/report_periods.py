'''Helpers for resolving month-selected report periods.'''

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
import re
from typing import TypedDict


class MonthOption(TypedDict):
    'One selectable calendar month option.'
    value: str
    label: str
    selected: bool


@dataclass(frozen=True)
class ReportPeriodContext:
    'Template and query context for month-selected reports.'
    period_start: datetime
    period_end: datetime
    report_period_label: str
    selected_month: date
    selected_month_value: str
    current_month_value: str
    previous_month_value: str
    month_options: list[MonthOption]

    def as_template_context(self) -> dict[str, object]:
        'Return a plain dict for Jinja template expansion.'
        return {
            'period_start': self.period_start,
            'period_end': self.period_end,
            'report_period_label': self.report_period_label,
            'selected_month': self.selected_month,
            'selected_month_value': self.selected_month_value,
            'current_month_value': self.current_month_value,
            'previous_month_value': self.previous_month_value,
            'month_options': self.month_options,
        }


def add_months(month_start: date, month_delta: int) -> date:
    'Return the first day of a month offset from another month start.'
    month_index = (month_start.year * 12) + (month_start.month - 1) + month_delta
    return date(month_index // 12, (month_index % 12) + 1, 1)


def resolve_report_month(raw_month: str | None, now: datetime | None = None) -> date:
    'Return selected report month, defaulting invalid or future values to current month.'
    reference_now = now or datetime.now()
    current_month = date(reference_now.year, reference_now.month, 1)
    if not raw_month:
        return current_month

    if not re.fullmatch(r'\d{4}-\d{2}', raw_month.strip()):
        return current_month

    try:
        year_text, month_text = raw_month.strip().split('-', 1)
        selected_month = date(int(year_text), int(month_text), 1)
    except ValueError:
        return current_month

    if selected_month > current_month:
        return current_month
    return selected_month


def get_report_month_period(month_start: date, now: datetime | None = None) -> tuple[datetime, datetime]:
    'Return inclusive datetime bounds for a selected report month.'
    reference_now = now or datetime.now()
    period_start = datetime.combine(month_start, time.min)
    next_month = add_months(month_start, 1)
    period_end = datetime.combine(next_month, time.min) - timedelta(microseconds=1)
    current_month = date(reference_now.year, reference_now.month, 1)
    if month_start == current_month:
        period_end = reference_now
    return period_start, period_end


def build_report_period_context(
    raw_month: str | None,
    available_months: list[date],
    now: datetime | None = None,
) -> ReportPeriodContext:
    'Build reusable template/query context for month-selected reports.'
    reference_now = now or datetime.now()
    selected_month = resolve_report_month(raw_month, reference_now)
    current_month = date(reference_now.year, reference_now.month, 1)
    previous_month = add_months(current_month, -1)
    month_options = sorted(
        set(available_months + [current_month, previous_month, selected_month]),
        reverse=True,
    )
    period_start, period_end = get_report_month_period(selected_month, reference_now)
    return ReportPeriodContext(
        period_start=period_start,
        period_end=period_end,
        report_period_label=selected_month.strftime('%B %Y'),
        selected_month=selected_month,
        selected_month_value=selected_month.strftime('%Y-%m'),
        current_month_value=current_month.strftime('%Y-%m'),
        previous_month_value=previous_month.strftime('%Y-%m'),
        month_options=[
            {
                'value': option.strftime('%Y-%m'),
                'label': option.strftime('%B %Y'),
                'selected': option == selected_month,
            }
            for option in month_options
        ],
    )
