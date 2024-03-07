import csv
import os
from datetime import datetime

from api.api import app
from api.db import db
from api.models import DetroitParcel
from sqlalchemy import text

DATA_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "database"
)

STORIES_MAP = {
    "1 to 1.5": 1,
    "1": 1,
    "1.5": 2,
    "2": 2,
    "1.5 to 2": 2,
    "1.5 to 2.5": 2,
    "3+": 3,
    "3": 3,
    "4": 4,
}
BATHS_MAP = {"1": 1, "1.5": 2, "2 to 3": 3, "3": 4, "4": 4, "3+": 4}
EXTERIOR_MAP = {"Siding": 1, "Brick/other": 2, "Brick": 3, "Other": 4}

with app.app_context():
    db.session.execute(text("CREATE EXTENSION IF NOT EXISTS postgis"))
    db.session.commit()
    db.create_all()
    db.session.commit()


if __name__ == "__main__":
    print(os.getenv("DATABASE_URL"))
    current_year = datetime.now().year
    detail_pin_map = {}

    with app.app_context():
        with open(os.path.join(DATA_DIR, "nez.csv"), "r") as f:
            reader = csv.DictReader(f)
            for row in reader:
                parcel = DetroitParcel.query.filter_by(pin=row["Parcel ID"]).first()
                create_parcel = parcel is None
                if create_parcel:
                    street_number, street_name = row["Address"].split(" ", 1)
                    parcel = DetroitParcel(
                        pin=row["Parcel ID"],
                        street_number=street_number,
                        street_name=street_name,
                    )
                    print("CREATING " + row["Parcel ID"])
                if row["Stories"].strip():
                    parcel.stories = STORIES_MAP.get(row["Stories"])
                if row["Exterior"].strip():
                    parcel.exterior = EXTERIOR_MAP.get(row["Exterior"])
                if row["Year built"].strip():
                    parcel.year_built = int(row["Year built"])
                    parcel.age = current_year - int(row["Year built"])
                if row["Baths"].strip():
                    parcel.baths = BATHS_MAP.get(row["Baths"])
                if row["Garage"].strip():
                    parcel.garage = row["Garage"] == "Yes"
                if row["Basement"].strip():
                    parcel.basement = row["Basement"] == "Yes"
                if row["Neighborhood"].strip():
                    parcel.neighborhood = row["Neighborhood"]
                if row["Sqft"].strip():
                    parcel.total_sq_ft = float(row["Sqft"].replace(",", ""))
                if row["Assessed Value"].strip():
                    parcel.assessed_value = float(
                        row["Assessed Value"].replace(",", "")
                    )
                # TODO:
                # parcel.sale_date
                if row["Sale Price"].strip():
                    parcel.sale_price = float(
                        row["Sale Price"].replace("$", "").replace(",", "")
                    )
                if create_parcel:
                    db.session.add(parcel)
                else:
                    db.session.merge(parcel)
        db.session.commit()
