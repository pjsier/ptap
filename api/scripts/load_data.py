import csv
import os
from datetime import datetime

from api.api import app
from api.db import db
from api.models import CookParcel, DetroitParcel
from sqlalchemy import text

DATA_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "database"
)

DETAIL_PINS = [
    "22017686.",
    "22100099.",
    "22090795-6",
    "27061142.",
    "27210198.",
    "27170111.",
    "17012650.003",
    "27130066.",
    "27060564.001",
    "12005857.",
    "27071737.",
    "27061566.",
    "27061239.",
    "27072125.001",
    "27071750.",
    "16041434.",
    "22052061.",
    "02005984.",
    "27140260.",
    "22047740.",
    "27061959.",
    "27080206.",
    "14002468.",
    "27190031.",
    "21063416-0",
    "27230331.",
    "21069359.",
    "21071713.",
    "21078802.",
    "27080420.",
    "27220061.",
    "27110287.",
    "21040000.034",
    "22095241-2",
    "21070894.",
    "22118203.",
]

with app.app_context():
    db.session.execute(text("CREATE EXTENSION IF NOT EXISTS postgis"))
    db.session.commit()
    db.create_all()
    db.session.commit()


if __name__ == "__main__":
    current_year = datetime.now().year
    with open(os.path.join(DATA_DIR, "cook.csv"), "r") as f:
        cook_parcels = []
        reader = csv.DictReader(f)
        for idx, row in enumerate(reader):
            # Skip vacant land
            if row["class"].strip() in ["100", "200"]:
                continue
            sale_price = (
                float(row["sale_price"])
                if row["sale_price"] not in ["", "NA"]
                else None
            )
            building_sq_ft = float(row["building_sqft"] or 0)
            land_sq_ft = float(row["land_sqft"] or 0)
            total_sq_ft = building_sq_ft + land_sq_ft
            price_per_sq_ft = None
            if sale_price:
                price_per_sq_ft = sale_price / total_sq_ft
            point = None
            if row["longitude"] and row["latitude"]:
                point = f"POINT({row['longitude']} {row['latitude']})"
            year_built = None
            age = None
            if row["year_built"]:
                year_built = int(row["year_built"])
                age = 2024 - year_built
            cook_parcels.append(
                CookParcel(
                    id=idx,
                    pin=row["pin"],
                    street_number=row["st_num"],
                    street_name=row["st_name"],
                    sale_price=sale_price,
                    sale_year=row["year"].replace(".0", "") or None,
                    assessed_value=row["certified_tot"]
                    if row["certified_tot"]
                    else None,
                    property_class=row["class"],
                    age=age,
                    year_built=year_built,
                    building_sq_ft=building_sq_ft,
                    land_sq_ft=land_sq_ft,
                    price_per_sq_ft=price_per_sq_ft,
                    rooms=row["num_rooms"] or None,
                    bedrooms=row["num_bedrooms"] or None,
                    exterior=row["exterior"].replace(".0", "")
                    if row["exterior"]
                    else None,
                    stories=row["stories_recode"] or None,
                    basement=row["basement_recode"] == "True",
                    garage=row["garage_indicator"] not in ["False", ""],
                    geom=point,
                )
            )
        with app.app_context():
            db.session.bulk_save_objects(cook_parcels)
            db.session.commit()

    detail_pin_map = {}
    with open(os.path.join(DATA_DIR, "detroit-old.csv"), "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row["parcel_num"] in DETAIL_PINS:
                detail_pin_map[row["parcel_num"]] = row

    with open(os.path.join(DATA_DIR, "detroit.csv"), "r") as f:
        detroit_parcels = []
        reader = csv.DictReader(f)
        for idx, row in enumerate(reader):
            point = None
            if row["Longitude"] not in ["", "NA"]:
                point = f"POINT({row['Longitude']} {row['Latitude']})"
            sale_price = float(row["Sale Price"]) if row["Sale Price"] != "" else None
            total_sq_ft = float(row["total_squa"]) if row["total_squa"] != "" else None
            price_per_sq_ft = None
            if sale_price and total_sq_ft and total_sq_ft > 0:
                price_per_sq_ft = sale_price / total_sq_ft
            year_built = (
                int(row["resb_yearbuilt"]) if row["resb_yearbuilt"] != "" else None
            )
            age = None
            if year_built:
                age = current_year - year_built
            sale_date = None
            sale_year = None
            if row["Sale Date"]:
                sale_date = datetime.strptime(row["Sale Date"][:10], "%Y-%m-%d").date()
                sale_year = sale_date.year
            parcel = DetroitParcel(
                id=idx,
                pin=row["parcel_num"],
                street_number=row["st_num"],
                street_name=row["st_name"],
                neighborhood=row["ECF"],
                assessed_value=float(row["ASSESSEDVALUETENTATIVE"]),
                taxable_value=float(row["TAXABLEVALUETENTATIVE"]),
                sale_price=sale_price,
                sale_date=sale_date,
                sale_year=sale_year,
                year_built=year_built,
                age=age,
                effective_age=int(row["resb_effage"]) if row["resb_effage"] else None,
                total_sq_ft=total_sq_ft,
                total_acreage=float(row["TOTALACREAGE"]) or None,
                total_floor_area=row["total_floor_area"]
                if row["total_floor_area"] not in ["", "NA"]
                else None,
                price_per_sq_ft=price_per_sq_ft,
                stories=row["heightcat"].replace(".0", "")
                if row["heightcat"] not in ["", "-1"]
                else None,
                baths=row["bathcat"].replace(".0", "")
                if row["bathcat"] not in ["", "-1"]
                else None,
                exterior=row["extcat"].replace(".0", "")
                if (row["extcat"] not in ["-1", ""])
                else None,
                basement="1" in row.get("has_basement", ""),
                garage="1" in row.get("has_garage", ""),
                taxpayer=row["TAXPAYER1"],
                homestead_exemption=row["TAXPAYER1"],
                geom=point,
            )
            if row["parcel_num"] in DETAIL_PINS:
                detail = detail_pin_map.get(row["parcel_num"], {})
                parcel.age = current_year - int(detail["year_built"])
                parcel.year_built = int(detail["year_build"])
                parcel.
                parcel.total_floor_area = float(detail["total_floor_area"])

            detroit_parcels.append(parcel)
        with app.app_context():
            db.session.bulk_save_objects(detroit_parcels)
            db.session.commit()
