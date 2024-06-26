import io
import json
import os
from datetime import datetime
from tempfile import NamedTemporaryFile, TemporaryDirectory

import boto3
import gspread
import pytz
import requests
import sentry_sdk
from docx.shared import Inches
from docxtpl import DocxTemplate, InlineImage
from google.oauth2 import service_account
from PIL import Image
from pillow_heif import register_heif_opener

from .constants import DETROIT_EXTERIOR_MAP, METERS_IN_MILE


def record_final_submission(submission):
    # TODO:
    if not os.getenv("GOOGLE_SERVICE_ACCOUNT"):
        return

    credentials_json = json.loads(os.getenv("GOOGLE_SERVICE_ACCOUNT"))
    credentials = service_account.Credentials.from_service_account_info(
        credentials_json,
        scopes=[
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive.file",
            "https://www.googleapis.com/auth/drive",
        ],
    )

    client = gspread.authorize(credentials)
    sheet_name = os.getenv("GOOGLE_SHEET_SUBMISSION_NAME")
    sheet_sid = os.getenv("GOOGLE_SHEET_SID")
    if submission.get("region") == "milwaukee":
        sheet_name = os.getenv("MKE_GOOGLE_SHEET_SUBMISSION_NAME")
        sheet_sid = os.getenv("MKE_GOOGLE_SHEET_SID")
    worksheet = client.open(sheet_name).worksheet("submissions")

    timestamp = datetime.now(pytz.timezone("America/Detroit"))
    key = f"submissions/{timestamp.strftime('%Y/%m/%d')}/{submission.get('uuid')}.json"
    # TODO: For now just append, in the future update like lambda for resubmission

    info = submission.get("user", {})
    eligibility = submission.get("eligibility", {})
    row_data = [
        submission.get("uuid"),
        key,
        timestamp.strftime("%Y-%m-%dT%H:%M:%SZ"),
        info.get("name", f'{submission["first_name"]} {submission["last_name"]}'),
        info.get("email"),
        info.get("phone"),
        info.get("phonetype"),
        submission.get("pin"),
        submission.get("address"),
        info.get("city"),
        info.get("state"),
        eligibility.get("residence"),
        eligibility.get("owner"),
        eligibility.get("hope"),
        info.get("mailingaddress"),
        info.get("altcontactname"),
        info.get("heardabout"),
        info.get("localinput"),
        info.get("socialmedia"),
        submission.get("validcharacteristics"),
        submission.get("characteristicsinput"),
        submission.get("valueestimate"),
        len(submission.get("selectedComparables", [])),
        submission.get("damage_level"),
        submission.get("damage"),
        len(submission.get("files", [])),
    ]
    worksheet.append_rows([row_data])

    val_list = worksheet.col_values(1)
    base_url = "https://docs.google.com/spreadsheets/d/"

    # TODO: Pull SID dynamically
    return f"{base_url}{sheet_sid}/edit#gid=0&range=A{len(val_list)}"


# TODO: Just use that as the request
def get_region(request_data):
    appeal_type = request_data.get("appeal_type", "")
    if "detroit" in appeal_type:
        return "detroit"
    if "cook" in appeal_type:
        return "cook"
    if "milwaukee" in appeal_type:
        return "milwaukee"
    return "detroit"


def load_s3_json(s3, bucket, key):
    data = json.load(s3.get_object(Bucket=bucket, Key=key)["Body"])
    if "agreement_date" not in data:
        data["agreement_date"] = data["timestamp"][:10]
    return data


def render_agreement(name, parcel, agreement_date):
    base_dir = os.path.dirname(os.path.abspath(__file__))
    doc = DocxTemplate(
        os.path.join(base_dir, "templates", "docs", "representation_agreement.docx")
    )

    if not agreement_date:
        timestamp = datetime.now(pytz.timezone("America/Detroit"))
        agreement_date = timestamp.strftime("%Y-%m-%d")
    # TODO: Only currently supports Detroit
    city_state = {"city": "Detroit", "state": "MI"}
    doc.render(
        {
            "agreement_date": agreement_date,
            "partner_name": name,
            "parcel_num": parcel.pin,
            "street_address": f"{parcel.street_number} {parcel.street_name}",
            **city_state,
        }
    )

    file_stream = io.BytesIO()
    doc.save(file_stream)
    file_stream.seek(0)
    return file_stream.getvalue()


def process_doc_images(doc, files, temp_dir):
    """Process images individually after upload"""
    register_heif_opener()

    MAX_WIDTH = Inches(5.5)
    MAX_HEIGHT = Inches(4)
    images = []
    for file in files:
        res = requests.get(file["url"])
        if res.status_code != 200:
            continue
        try:
            img = Image.open(io.BytesIO(res.content)).convert("RGB")
        except Exception as e:
            sentry_sdk.capture_exception(e)
            continue
        temp_file = NamedTemporaryFile(dir=temp_dir, suffix=".jpg", delete=False)
        img.save(temp_file.name, format="JPEG")
        # Only constrain the larger dimension
        img_kwargs = (
            {"height": MAX_HEIGHT} if img.height > img.width else {"width": MAX_WIDTH}
        )
        images.append(InlineImage(doc, temp_file.name, **img_kwargs))
    return images


def render_doc_to_bytes(doc, context, files):
    """Handle common processing methods for rendering a doc, including images"""
    with TemporaryDirectory() as temp_dir:
        images = process_doc_images(doc, files, temp_dir)
        doc.render({**context, "images": images, "has_images": len(images) > 0})
        # also save a byte object to return
        file_stream = io.BytesIO()
        doc.save(file_stream)  # save to stream
        file_stream.seek(0)  # reset pointer to head
    return file_stream.getvalue()


def log_step(logger, data):
    # TODO: Seems like some LOG_STEPs are getting ignored
    logger.info(f"LOG_STEP: {json.dumps(data)}")
    # Only running for specific steps to reduce latency
    if data.get("step") in ["agreement", "submit"]:
        update_s3_submission(data)


def update_s3_submission(data):
    if not data.get("uuid") or not os.getenv("S3_SUBMISSIONS_BUCKET"):
        return
    timestamp = datetime.now(pytz.timezone("America/Detroit"))
    data["timestamp"] = timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")
    timestamp_path = timestamp.strftime("%Y/%m/%d")

    s3 = boto3.client("s3")
    s3.put_object(
        Body=json.dumps(data),
        Bucket=os.getenv("S3_SUBMISSIONS_BUCKET"),
        Key=f"submissions/{timestamp_path}/{data.get('uuid')}.json",
    )


def clean_cook_parcel(parcel):
    exterior_map = {1: "Wood", 2: "Masonry", 3: "Wood/Masonry", 4: "Stucco"}
    if "distance" in parcel:
        parcel["distance"] = "{:0.2f}mi".format(parcel["distance"] / METERS_IN_MILE)
    return {
        **parcel,
        "assessed_value": "{:,.0f}".format(parcel["assessed_value"]),
        "building_sq_ft": "{:,.0f}".format(parcel["building_sq_ft"]),
        "basement": "Yes" if parcel["basement"] else "No",
        "exterior": exterior_map.get(parcel["exterior"]),
        "garage": "Yes" if parcel["garage"] else "No",
    }


def clean_detroit_parcel(parcel):
    if "distance" in parcel:
        parcel["distance"] = "{:0.2f}mi".format(parcel["distance"] / METERS_IN_MILE)
    data = {
        **parcel,
        "sale_price": "${:,.0f}".format(parcel["sale_price"])
        if parcel.get("sale_price")
        else "",
        "total_sq_ft": "{:,.0f}".format(parcel["total_sq_ft"])
        if parcel.get("total_sq_ft")
        else "",
        "exterior_display": DETROIT_EXTERIOR_MAP.get(parcel.get("exterior"), ""),
    }
    if parcel.get("assessed_value"):
        data["assessed_value"] = "{:,.0f}".format(parcel["assessed_value"])
    return data


def clean_milwaukee_parcel(parcel):
    if "distance" in parcel:
        parcel["distance"] = "{:0.2f}mi".format(parcel["distance"] / METERS_IN_MILE)
    data = {
        **parcel,
        "sale_price": "${:,.0f}".format(parcel["sale_price"])
        if parcel.get("sale_price")
        else "",
        "total_sq_ft": "{:,.0f}".format(parcel["total_sq_ft"])
        if parcel.get("total_sq_ft")
        else "",
    }
    if isinstance(parcel.get("sale_date"), str):
        parcel["sale_date"] = parcel["sale_date"][:10]
    if parcel.get("assessed_value"):
        data["assessed_value"] = "{:,.0f}".format(parcel["assessed_value"])
    if (parcel.get("half_baths") or 0) > 0:
        parcel["baths"] = f"{parcel['baths']}.{parcel['half_baths']}"
    return data
