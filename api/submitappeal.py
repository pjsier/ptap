import os
from datetime import datetime

import pandas as pd
import pytz
from docxtpl import DocxTemplate

from .constants import DAMAGE_TO_CONDITION, METERS_IN_MILE
from .dataqueries import _get_pin, _get_pin_with_distance
from .email import (
    cook_submission_email,
    detroit_internal_submission_email,
    detroit_submission_email,
    milwaukee_submission_email,
)
from .utils import (
    clean_cook_parcel,
    clean_detroit_parcel,
    clean_milwaukee_parcel,
    record_final_submission,
    render_doc_to_bytes,
)

gsheet_submission = None


def submit_cook_sf(comp_submit, mail):
    owner_name = comp_submit.get(
        "name", f'{comp_submit["first_name"]} {comp_submit["last_name"]}'
    )

    t_df = pd.DataFrame([comp_submit["target_pin"]])
    comps_df = pd.DataFrame(comp_submit["selectedComparables"])

    pin_av = t_df.assessed_value[0]
    pin = t_df.pin[0]
    comps_avg = comps_df["assessed_value"].mean()

    base_dir = os.path.dirname(os.path.abspath(__file__))

    # generate docx
    output_name = (
        f"{pin} Protest Letter Updated {datetime.today().strftime('%m_%d_%y')}.docx"
    )
    comp_submit["output_name"] = output_name
    doc = DocxTemplate(
        os.path.join(base_dir, "templates", "docs", "cook_template_2024.docx")
    )

    context = {
        "date": datetime.now(pytz.timezone("America/Chicago")).strftime("%B %-d, %Y"),
        "pin": pin,
        "address": comp_submit["address"],
        "homeowner_name": owner_name,
        "assessor_av": "{:,.0f}".format(pin_av),
        "assessor_mv": "${:,.0f}".format(pin_av * 10),
        "contention_av": "{:,.0f}".format(comps_avg),
        "contention_mv": "${:,.0f}".format(comps_avg),
        "target": clean_cook_parcel(t_df.to_dict(orient="records")[0]),
        "comparables": [
            clean_cook_parcel(p) for p in comps_df.to_dict(orient="records")
        ],
    }

    # TODO: What is this used for?
    output = {}

    comp_submit["file_stream"] = render_doc_to_bytes(doc, context, comp_submit["files"])

    if os.getenv("GOOGLE_SHEET_SID"):
        comp_submit["log_url"] = "https://docs.google.com/spreadsheets/d/" + os.getenv(
            "GOOGLE_SHEET_SID"
        )

    # send email
    cook_submission_email(mail, comp_submit)

    return output


def submit_detroit_sf(comp_submit, mail):
    owner_name = comp_submit.get(
        "name", f'{comp_submit["first_name"]} {comp_submit["last_name"]}'
    )
    comps_df = pd.DataFrame(comp_submit["selectedComparables"])
    pin = comp_submit["target_pin"]["pin"]
    if "sale_price" not in comps_df:
        comps_df["sale_price"] = 0
    comps_avg = comps_df["sale_price"].mean()

    # generate docx
    base_dir = os.path.dirname(os.path.abspath(__file__))

    comp_submit[
        "output_name"
    ] = f"{pin} Protest Letter Updated {datetime.today().strftime('%m_%d_%y')}.docx"
    doc = DocxTemplate(
        os.path.join(base_dir, "templates", "docs", "detroit_template_2024.docx")
    )

    target = _get_pin("detroit", pin)
    primary, primary_distance = _get_pin_with_distance(
        "detroit", comp_submit.get("selected_primary"), target
    )

    if not os.getenv("ATTACH_LETTERS"):
        detroit_submission_email(mail, comp_submit, None)
        if not comp_submit.get("resumed"):
            detroit_internal_submission_email(mail, comp_submit, None)
        return

    comparables = comps_df.to_dict(orient="records")

    primary_pin = primary.pin if primary else ""
    has_comparables = len([c for c in comparables if c["pin"] != primary_pin]) > 0

    context = {
        "pin": pin,
        "owner": owner_name,
        "address": comp_submit["address"],
        "formal_owner": owner_name,
        "current_faircash": "${:,.0f}".format(target.assessed_value * 2),
        "contention_faircash2": "${:,.0f}".format(comps_avg),
        "target": clean_detroit_parcel(target.as_dict()),
        "primary": clean_detroit_parcel(primary.as_dict() if primary else {}),
        "has_primary": primary is not None,
        "has_comparables": has_comparables,
        "comparables": [clean_detroit_parcel(p) for p in comparables],
        "year": 2024,
        "economic_obsolescence": comp_submit.get("economic_obsolescence"),
        **detroit_depreciation(
            target.age,
            target.effective_age,
            comp_submit["damage"],
            comp_submit["damage_level"],
        ),
        **primary_details(primary, primary_distance),
    }

    letter_bytes = render_doc_to_bytes(doc, context, comp_submit["files"])

    if not comp_submit.get("resumed"):
        detroit_internal_submission_email(mail, comp_submit, letter_bytes)
    detroit_submission_email(mail, comp_submit, letter_bytes)

    return {}


def detroit_depreciation(actual_age, effective_age, damage, damage_level):
    condition = DAMAGE_TO_CONDITION.get(damage_level, [0, 0, 0])
    percent_good = 100 - effective_age
    schedule_incorrect = effective_age < actual_age and not (
        actual_age >= 55 and effective_age >= 55
    )
    damage_incorrect = condition[2] < percent_good
    damage_correct = condition[0] > percent_good

    assessor_damage_level = get_damage_level(percent_good).title().replace("_", " ")
    capped_age = min(actual_age, 55)
    return {
        "age": actual_age,
        "capped_age": capped_age,
        "capped_percent_good": 100 - capped_age,
        "effective_age": effective_age,
        "new_effective_age": 100 - condition[1],
        "percent_good": percent_good,
        "assessor_damage_level": assessor_damage_level,
        "schedule_incorrect": schedule_incorrect,
        "damage": damage,
        "damage_level": damage_level.title().replace("_", " "),
        "damage_midpoint": condition[1],
        "damage_incorrect": damage_incorrect,
        "damage_correct": damage_correct,
        "show_depreciation": damage_incorrect,
    }


def primary_details(primary, primary_distance):
    if not primary:
        return {}
    if primary_distance:
        primary_distance = "{:0.2f}mi".format(primary_distance / METERS_IN_MILE)
    return {
        "primary_distance": primary_distance or "",
        "contention_faircash": "${:,.0f}".format(primary.sale_price)
        if primary.sale_price
        else "",
        "contention_sev": "{:,.0f}".format(primary.sale_price / 2)
        if primary.sale_price
        else "",
        "primary_sale_price": "${:,.0f}".format(primary.sale_price)
        if primary.sale_price
        else "",
        "primary_sale_date": primary.sale_date.strftime("%Y-%m-%d")
        if primary.sale_date
        else "",
    }


def get_damage_level(percent_good):
    for level, value_range in DAMAGE_TO_CONDITION.items():
        if percent_good >= value_range[0] and percent_good <= value_range[2]:
            return level
    return ""


def submit_milwaukee_sf(comp_submit, mail):
    owner_name = comp_submit.get(
        "name", f'{comp_submit["first_name"]} {comp_submit["last_name"]}'
    )
    comps_df = pd.DataFrame(comp_submit["selectedComparables"])
    pin = comp_submit["target_pin"]["pin"]
    if "sale_price" not in comps_df:
        comps_df["sale_price"] = 0
    comps_avg = comps_df["sale_price"].mean()

    base_dir = os.path.dirname(os.path.abspath(__file__))

    comp_submit[
        "output_name"
    ] = f"{pin} Protest Letter Updated {datetime.today().strftime('%m_%d_%y')}.docx"
    doc = DocxTemplate(
        os.path.join(base_dir, "templates", "docs", "milwaukee_template_2024.docx")
    )

    target = _get_pin("milwaukee", pin)
    primary, primary_distance = _get_pin_with_distance(
        "milwaukee", comp_submit.get("selected_primary"), target
    )

    comparables = comps_df.to_dict(orient="records")

    if primary is None:
        primary, primary_distance = _get_pin_with_distance(
            "milwaukee", comparables["pin"], target
        )
    primary_pin = primary.pin if primary else ""
    has_comparables = len([c for c in comparables if c["pin"] != primary_pin]) > 0

    context = {
        "pin": pin,
        "owner": owner_name,
        "address": comp_submit["address"],
        "formal_owner": owner_name,
        "current_faircash": "${:,.0f}".format(target.assessed_value * 2),
        "contention_faircash2": "${:,.0f}".format(comps_avg),
        "target": clean_milwaukee_parcel(target.as_dict()),
        "primary": clean_milwaukee_parcel(primary.as_dict() if primary else {}),
        "has_primary": primary is not None,
        "has_comparables": has_comparables,
        "comparables_count": len(comparables),
        "comparables": [clean_milwaukee_parcel(p) for p in comparables],
        "year": 2024,
        **primary_details(primary, primary_distance),
    }

    letter_bytes = render_doc_to_bytes(doc, context, comp_submit["files"])

    if os.getenv("GOOGLE_SHEET_SID"):
        comp_submit["log_url"] = "https://docs.google.com/spreadsheets/d/" + os.getenv(
            "GOOGLE_SHEET_SID"
        )

    if not comp_submit.get("resumed"):
        record_final_submission(comp_submit)
    milwaukee_submission_email(mail, comp_submit, letter_bytes)

    return {}
