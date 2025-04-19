import csv
import os
import sys
from datetime import datetime

import geopandas as gpd
import pandas as pd
from sqlalchemy import text
from sqlalchemy.types import Integer

from api import db
from api.api import app
from api.models import CookParcel, DetroitParcel

DATA_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "database"
)
DETROIT_EXTERIOR_MAP = {1: "Siding", 2: "Brick/other", 3: "Brick", 4: "Other"}

current_year = datetime.now().year

with app.app_context():
    db.session.execute(text("CREATE EXTENSION IF NOT EXISTS postgis"))
    db.session.commit()
    db.session.execute(text("CREATE EXTENSION IF NOT EXISTS pg_trgm"))
    db.session.commit()
    db.create_all()
    db.session.commit()


def load_cook():
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
                age = 2025 - year_built
            cook_parcels.append(
                CookParcel(
                    id=idx,
                    pin=row["pin"],
                    street_number=row["st_num"],
                    street_name=row["st_name"],
                    street_address=f"{row['st_num']} {row['st_name']}",
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


def load_detroit():
    with open(os.path.join(DATA_DIR, "detroit-2025.csv"), "r") as f:
        detroit_parcels = []
        reader = csv.DictReader(f)
        for idx, row in enumerate(reader):
            point = None
            if row["Longitude"] not in ["", "NA"]:
                point = f"POINT({row['Longitude']} {row['Latitude']})"
            sale_price = (
                float(row["SALEPRICE"]) if row["SALEPRICE"] not in ["", "NA"] else None
            )
            total_sq_ft = (
                float(row["TOTALSQFT"]) if row["TOTALSQFT"] not in ["", "NA"] else None
            )
            price_per_sq_ft = None
            if sale_price and total_sq_ft and total_sq_ft > 0:
                price_per_sq_ft = sale_price / total_sq_ft
            year_built = (
                int(float(row["resb_yearbuilt"]))
                if row["resb_yearbuilt"] != ""
                else None
            )
            age = None
            if year_built:
                age = current_year - year_built
            sale_date = None
            sale_year = None
            if row["SALEDATE"]:
                sale_date = datetime.strptime(row["SALEDATE"][:10], "%Y-%m-%d").date()
                sale_year = sale_date.year
            addr_split = row["PROPADDR"].split(" ")

            detroit_parcels.append(
                DetroitParcel(
                    id=idx,
                    pin=row["parcel_num"],
                    street_number=addr_split[0],
                    street_name=" ".join(addr_split[1:]),
                    street_address=row["PROPADDR"],
                    neighborhood=row["ECF"],
                    assessed_value=float(row["ASSESSEDVALUETENTATIVE"])
                    if row["ASSESSEDVALUETENTATIVE"]
                    else None,
                    taxable_value=float(row["TAXABLEVALUETENTATIVE"])
                    if row["TAXABLEVALUETENTATIVE"]
                    else None,
                    sale_price=sale_price,
                    sale_date=sale_date,
                    sale_year=sale_year,
                    year_built=year_built,
                    age=age,
                    effective_age=int(float(row["resb_effage"]))
                    if row["resb_effage"]
                    else None,
                    total_sq_ft=total_sq_ft,
                    total_acreage=float(row["TOTALACREAGE"])
                    if row["TOTALACREAGE"]
                    else None,
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
                    homestead_exemption="",
                    geom=point,
                )
            )
        with app.app_context():
            db.session.bulk_save_objects(detroit_parcels)
            db.session.commit()


def load_milwaukee():
    df = pd.read_csv(
        os.path.join(DATA_DIR, "mke-2025.csv"),
        dtype={
            "TAXKEY": "str",
            "NEIGHBORHOOD": "str",
            "HOUSE_NR_LO": "str",
            "POWDER_ROOMS": "int",
        },
        parse_dates=["CONVEY_DATE"],
    ).rename(columns={"TAXKEY": "ParcelID"})
    # https://data.milwaukee.gov/dataset/mprop/resource/3d94613b-a1df-4ed1-86d9-d9205925e2ab
    # Fee is price * 0.003, so reverse that here
    df["CONVEY_PRICE"] = df["CONVEY_FEE"] / 0.003
    parcel_geom_df = gpd.read_file(os.path.join(DATA_DIR, "mke-parcel-points.geojson"))

    parcel_df = parcel_geom_df.merge(
        df,
        on=["ParcelID"],
        how="left",
    ).rename(
        columns={
            "ParcelID": "pin",
            "HOUSE_NR_LO": "street_number",
            "C_A_TOTAL": "assessed_value",
            "CONVEY_DATE": "sale_date",
            "CONVEY_PRICE": "sale_price",
            "YR_BUILT": "year_built",
            "BATHS": "baths",
            "POWDER_ROOMS": "half_baths",
            "BEDROOMS": "bedrooms",
            "NEIGHBORHOOD": "neighborhood",
            "BLDG_AREA": "total_sq_ft",
            "LAND_USE_GP": "building_category",
            "BLDG_TYPE": "building_type",
            "geometry": "geom",
        }
    )

    parcel_df = parcel_df[parcel_df.geom.is_valid]
    parcel_df["id"] = parcel_df.reset_index().index
    parcel_df["street_name"] = parcel_df.apply(
        lambda row: " ".join(
            [str(val) for val in row[["SDIR", "STREET", "STTYPE"]] if pd.notnull(val)]
        ),
        axis=1,
    )
    parcel_df["street_address"] = (
        parcel_df["street_number"].astype(str) + " " + parcel_df["street_name"]
    )
    parcel_df["sale_year"] = parcel_df["sale_date"].dt.year.astype("Int64")
    parcel_df["age"] = (
        parcel_df["year_built"]
        .apply(lambda y: current_year - y if y else y)
        .astype("Int64")
    )
    parcel_df["price_per_sq_ft"] = (
        parcel_df["assessed_value"] / parcel_df["total_sq_ft"]
    )
    parcel_df = parcel_df.astype(
        {
            "year_built": "Int64",
            "bedrooms": "Int64",
            "baths": "Int64",
            "half_baths": "Int64",
        }
    )
    parcel_df = parcel_df[
        [
            "id",
            "pin",
            "street_number",
            "street_name",
            "street_address",
            "neighborhood",
            "assessed_value",
            "sale_price",
            "sale_date",
            "sale_year",
            "age",
            "year_built",
            "total_sq_ft",
            "price_per_sq_ft",
            "bedrooms",
            "baths",
            "half_baths",
            "building_category",
            "building_type",
            "geom",
        ]
    ].set_geometry("geom")

    with app.app_context():
        parcel_df.to_postgis(
            "milwaukee",
            db.engine,
            dtype={
                "sale_year": Integer(),
                "age": Integer(),
                "year_built": Integer(),
                "bedrooms": Integer(),
                "baths": Integer(),
                "half_baths": Integer(),
            },
            if_exists="replace",
            index=False,
        )


if __name__ == "__main__":
    if len(sys.argv) == 1:
        load_cook()
        load_detroit()
        load_milwaukee()
    elif sys.argv[1] == "cook":
        load_cook()
    elif sys.argv[1] == "detroit":
        load_detroit()
    elif sys.argv[1] == "mke":
        load_milwaukee()
