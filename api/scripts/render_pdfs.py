import json
import os

import gspread
from api.api import app
from api.db import db
from api.models import DetroitParcel
from google.oauth2 import service_account
from pypdf import PdfReader, PdfWriter
from sqlalchemy import text

BATCH = "batch4"

ADDR_CLEANUP_MAP = {
    "19441 BRETTON DR": "19441 BRETTON",
    "7474 LA SALLE BLVD": "7474 LASALLE BLVD",
    "19505 WARWICK ST": "19505 WARWICK",
    "5842 CASPER ST": "5842 CASPER",
    "8919": "8919 PREST",
    "10101 CURTIS STREET": "10101 CURTIS",
    "15821 PINEHURST STREET": "15821 PINEHURST",
    "12344 DUCHESS ST": "12344 DUCHESS",
    "18171 DEQUINDRE STREET": "18171 DEQUINDRE",
    "15113 MARK TWAIN ST": "15113 MARK TWAIN",
    "9643 ROBSTON STREET": "9643 ROBSON",
    "5910 COPLIN ST": "5910 COPLIN",
    # "13954 APPOLINE"
    "4060 MONTGOMERY ST": "4060 MONTGOMERY",
    "14775 CORAM ST": "14775 CORAM",
    "12550 LONGVIEW STREET": "12550 LONGVIEW",
    "13538 PENROD ST": "13538 PENROD",
    "14834 ROBSON ST": "14834 ROBSON",
    "19008 KENTFIELD ST": "19008 KENTFIELD",
    "5566 CANTON ST": "5566 CANTON",
    "20134 SAN JUAN DR": "20134 SAN JUAN",
    "18915 WARRINGTON DR": "18915 WARRINGTON",
    "18926 BRETTON DR": "18926 BRETTON",
    "20164 WARD ST": "20164 WARD",
    "18446 SUNDERLAND RD": "18446 SUNDERLAND",
    "26301 W. OUTER DRIVE": "26301 W OUTER DR",
    "26301 W OUTER DRIVE": "26301 W OUTER DR",
    "5510 OLDTOWN ST": "5510 OLDTOWN",
    "19763 GREYDALE AVE": "19763 GREYDALE",
    "12672 GALLAGHER ST.": "12672 GALLAGHER",
    "309 ASHLAND ST": "309 ASHLAND",
    "10360 E OUTER DRIVE": "10360 E OUTER DR",
    "5560 HELEN STREET": "5560 HELEN",
    "4775 IROQUOIS ST.": "4775 IROQUOIS",
    "19160 JOANN ST": "19160 JOANN",
    "3903 GRAYTON ST.": "3903 GRAYTON",
    "503 ROSEDALE COURT": "503 ROSEDALE CT",
    "9167 YORKSHIRE RD": "9167 YORKSHIRE",
    "24339 LEEWIN ST": "24339 LEEWIN",
    "2449 HAZELWOOD STREET": "2449 HAZELWOOD",
    "5603 E OUTER DRIVE": "5603 E OUTER DR",
    "17319 PATTON ST.": "17319 PATTON",
    "3626 SOMERSET AVE": "3626 SOMERSET",
    "19340 MONTE VISTA ST": "19340 MONTE VISTA",
    "17527 ANNCHESTER ROAD": "17527 ANNCHESTER",
    "19136 GRIGGS STREET": "19136 GRIGGS",
    "17376 OHIO STREET": "17376 OHIO",
    "19333 BARLOW ST": "19333 BARLOW",
    "3160 S. LIDDESDALE": "3160 LIDDESDALE",
    "14208 MANSFIELD ST.": "14208 MANSFIELD",
    "14816 FLANDERS STREET": "14816 FLANDERS",
    "156 WORCESTER PLACE": "156 WORCESTER PL",
    "16904 SAN JUAN DRIVE": "16904 SAN JUAN",
    "12020 LAING ST": "12020 LAING",
    "5851 OUTER DR E": "5851 E OUTER DR",
    "17681 TEPPERT ST": "17681 TEPPERT",
    "4885 BUCKINGHAM AVENUE": "4885 BUCKINGHAM",
    "8908 ARCHDALE ST": "8908 ARCHDALE",
    "20400 STRATFORD RD": "20400 STRATFORD",
    "19401 MARX ST.": "19401 MARX",
    "17370 STOEPEL ST.": "17370 STOEPEL",
    "6540 GRANDVILLE AVE": "6540 GRANDVILLE",
    "18651 WARRINGTON DR": "18651 WARRINGTON",
    "14910 LAMPHERE ST": "14910 LAMPHERE",
    "16230 APPOLINE ST": "16230 APPOLINE",
}

"""
Some logs generated twice, second doesn't have primary. Take the first?
i.e. 2747 STURTEVANT
"""

with app.app_context():
    db.session.execute(text("CREATE EXTENSION IF NOT EXISTS postgis"))
    db.session.commit()
    db.create_all()
    db.session.commit()

PDF_TEMPLATE = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "templates",
    "docs",
    "detroit_l4035f.pdf",
)

DATA_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "database"
)
BASE_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "data"
)

BATCH_FOLDER = os.getenv("PDF_BATCH_FOLDER", "1RUceKram1tj3riPVz1Dc64gGbPb24iKm")


def load_address_map():
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
    worksheet = client.open("PTAP Submissions").worksheet("Processor Assignments")
    address_map = {}
    log_worksheet = client.open("PTAP Submissions").worksheet("logs")
    log_map = {}
    for rec in worksheet.get_all_records():
        addr = rec["Address"].upper().strip()
        address_map[ADDR_CLEANUP_MAP.get(addr, addr)] = rec

    for rec in log_worksheet.get_all_records():
        if rec["step"] == "submit":
            addr = str(rec["address"]).upper().strip()
            log_map[ADDR_CLEANUP_MAP.get(addr, addr)] = json.loads(rec["data"])

    return address_map, log_map


def load_address_folders():
    address_folder_map = {}
    for dir in os.listdir(os.path.join(DATA_DIR, BATCH)):
        address = dir.split("-")[0].strip()
        address_folder_map[address] = os.path.join(DATA_DIR, BATCH, dir)
    return address_folder_map


def fill_pdf(address, address_folder, address_rec, log_rec):
    street_number, street_name = address.strip().split(" ", 1)

    if not address_rec:
        print(f"Address record not found for '{address}'")
        address_folder_new = address_folder + " - FIX"
        os.rename(address_folder, address_folder_new)
        return

    if not log_rec:
        print(f"Log record not found for '{address}'")

    if "selected_primary" not in (log_rec or {}):
        print(f"Log record does not have primary for '{address}'")

    data_dict = {
        "owner.name": address_rec["Partner Name"]
        if ("Yes" in address_rec.get("Name on Deed"))
        else "",
        "petitioner.name": address_rec["Partner Name"],
    }

    primary = None
    if log_rec and "selected_primary" in (log_rec or {}):
        with app.app_context():
            parcel = DetroitParcel.query.filter_by(
                street_number=street_number, street_name=street_name
            ).first()
            # if address == "19362 CHAPEL":
            #     breakpoint()
            primary = DetroitParcel.query.filter_by(
                pin=log_rec["selected_primary"]
            ).first()

            if not parcel:
                check_street_number, check_street_name = ADDR_CLEANUP_MAP.get(
                    address, address
                ).split(" ", 1)
                parcel = DetroitParcel.query.filter_by(
                    street_number=check_street_number, street_name=check_street_name
                ).first()
            if parcel:
                data_dict.update(
                    {
                        "prop.id": parcel.pin,
                        "assess.amt": parcel.assessed_value,
                        "value": parcel.taxable_value,
                    }
                )

    reader = PdfReader(PDF_TEMPLATE)
    writer = PdfWriter()
    writer.append(reader)

    if primary:
        data_dict["ownertrue.cash"] = "${:,.0f}".format(primary.sale_price)

    # if log_rec.get("selected_primary"):
    #     data_dict["ownertrue.cash"] = "${:,.0f}".format(
    #         s3_data["selected_primary"]["sale_price"]
    #     )
    # elif len(s3_data.get("selectedComparables", [])) > 0:
    #     data_dict["ownertrue.cash"] = "${:,.0f}".format(
    #         sum([c["sale_price"] for c in s3_data["selectedComparables"]])
    #         / len(s3_data["selectedComparables"])
    #     )

    # if "ownertrue.cash" in data_dict:
    #     print("has cash value for " + address)

    writer.update_page_form_field_values(
        writer.pages[0],
        data_dict,
        auto_regenerate=False,
    )

    if not primary:
        address_folder_new = address_folder + " - FIX"
        os.rename(address_folder, address_folder_new)
        address_folder = address_folder_new

    with open(os.path.join(address_folder, f"{address}.pdf"), "wb") as f:
        writer.write(f)


def main():
    address_map, log_map = load_address_map()
    address_folder_map = load_address_folders()
    for address, address_folder in address_folder_map.items():
        check_addr = ADDR_CLEANUP_MAP.get(address, address)
        fill_pdf(
            address,
            address_folder,
            address_map.get(check_addr),
            log_map.get(check_addr),
        )

    # fill_pdf({})
    # GOOGLE_CREDENTIALS = service_account.Credentials.from_service_account_info(
    #     json.loads(os.getenv("GOOGLE_SERVICE_ACCOUNT", "")),
    #     scopes=["https://www.googleapis.com/auth/drive"],
    # )

    # DRIVE_CLIENT = googleapiclient.discovery.build(
    #     "drive",
    #     "v2",
    #     credentials=GOOGLE_CREDENTIALS,
    #     cache_discovery=False,
    # )
    # results = (
    #     DRIVE_CLIENT.files()
    #     .list(
    #         q="sharedWithMe",
    #         # q=f"'1v_-aqBjTR5V3uiaSqmApWlx0tZwsYnxS' in parents",  # noqa
    #         fields="nextPageToken, files(id, name, mimeType)",
    #     )
    #     .execute()
    # )
    # print(results)
    # folders = results.get("files", [])
    # # TODO: Create mapping of addresses to folder IDs
    # for folder in folders:
    #     print(folder)


if __name__ == "__main__":
    main()
