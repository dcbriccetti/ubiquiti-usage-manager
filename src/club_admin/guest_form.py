'''Configurable printable guest form definitions.'''

from dataclasses import dataclass
from pathlib import Path
import tomllib
from typing import Any


DEFAULT_LABELS = {
    "name": "Name",
    "last_name": "Last Name",
    "first_name": "First Name",
    "nickname": "Nickname or Preferred Name",
    "date_of_birth": "Date of Birth",
    "address": "Street Address",
    "city": "City",
    "state": "State",
    "zip": "Zip Code",
    "cell_phone": "Cell Phone",
    "other_phone": "Other Phone",
    "email": "Email Address",
    "marital_status": "Marital Status",
    "single": "Single",
    "married": "Married",
    "recognized_couple": "Recognized Couple",
    "partner_name": "My partner with me today is",
    "guest_of_member": "Guest of a member?",
    "member_name": "Member Name",
    "heard_about": "How did you hear about us?",
    "newsletter_opt_out": "Do not sign me up for the newsletter",
    "printed_name": "Printed Name",
    "signature": "Signature",
    "date": "Date",
    "visit_date": "Visit Date",
}


DEFAULT_AGREEMENT_PARAGRAPHS = (
    "In consideration for guest access to this organization's property, facilities, activities, and/or events, I agree to comply with all rules, regulations, policies, and procedures.",
    "I release the organization, its employees, officers, directors, volunteers, and agents from claims resulting from my guest access to the property, facilities, activities, and/or events.",
    "I understand that as a guest I have no right to remain on the property and may be asked to leave at any time without cause.",
    "I am 18 years or older. I have read and agree to the terms and conditions set forth on this form.",
)


@dataclass(frozen=True, kw_only=True)
class GuestFormSpec:
    '''Content used to generate the printable guest form.'''

    title: str
    subtitle: str
    labels: dict[str, str]
    agreement_title: str
    agreement_paragraphs: tuple[str, ...]
    version: str


def default_guest_form_spec() -> GuestFormSpec:
    return GuestFormSpec(
        title="Guest Registration",
        subtitle="",
        labels=dict(DEFAULT_LABELS),
        agreement_title="Agreement and Release",
        agreement_paragraphs=DEFAULT_AGREEMENT_PARAGRAPHS,
        version="",
    )


def _string_value(value: Any, fallback: str) -> str:
    return value.strip() if isinstance(value, str) and value.strip() else fallback


def load_guest_form_spec(definition_path: str) -> GuestFormSpec:
    '''Load a guest form definition from TOML, or return a generic default.'''
    spec = default_guest_form_spec()
    if not definition_path.strip():
        return spec

    path = Path(definition_path).expanduser().resolve(strict=False)
    if not path.is_file():
        return spec

    try:
        data = tomllib.loads(path.read_text(encoding="utf-8"))
    except (OSError, tomllib.TOMLDecodeError):
        return spec
    labels = dict(spec.labels)
    raw_labels = data.get("labels") or data.get("fields") or {}
    if isinstance(raw_labels, dict):
        labels.update(
            {
                str(key): value.strip()
                for key, value in raw_labels.items()
                if isinstance(value, str) and value.strip()
            }
        )

    raw_agreement = data.get("agreement") or {}
    if not isinstance(raw_agreement, dict):
        raw_agreement = {}
    raw_paragraphs = raw_agreement.get("paragraphs")
    agreement_paragraphs = spec.agreement_paragraphs
    if isinstance(raw_paragraphs, list):
        agreement_paragraphs = tuple(
            paragraph.strip()
            for paragraph in raw_paragraphs
            if isinstance(paragraph, str) and paragraph.strip()
        ) or agreement_paragraphs

    return GuestFormSpec(
        title=_string_value(data.get("title"), spec.title),
        subtitle=_string_value(data.get("subtitle"), spec.subtitle),
        labels=labels,
        agreement_title=_string_value(
            raw_agreement.get("title"),
            spec.agreement_title,
        ),
        agreement_paragraphs=agreement_paragraphs,
        version=_string_value(data.get("version"), spec.version),
    )
