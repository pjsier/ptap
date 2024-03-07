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

EXTERIOR_MAP = {
    "21030462.": 3,
    "22065978.": 3,
    "13022975.": 3,
}

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
    print(os.getenv("DATABASE_URL"))
    current_year = datetime.now().year
    detail_pin_map = {}
    with open(os.path.join(DATA_DIR, "detroit-old.csv"), "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row["parcel_num"] in DETAIL_PINS:
                detail_pin_map[row["parcel_num"]] = row

    with app.app_context():
        for pin, exterior in EXTERIOR_MAP.items():
            parcel = DetroitParcel.query.filter_by(pin=row["parcel_num"]).first()
            print(parcel.exterior)
            parcel.exterior = exterior
            db.session.merge(parcel)

        for row in detail_pin_map.values():
            parcel = DetroitParcel.query.filter_by(pin=row["parcel_num"]).first()
            print(parcel.pin, row["year_built"])
            try:
                parcel.age = current_year - int(row["year_built"])
                parcel.year_built = int(row["year_built"])
            except Exception:
                pass

            parcel.total_sq_ft = float(row["total_squa"])
            parcel.total_floor_area = float(row["total_squa"])
            parcel.stories = (
                row["heightcat"].replace(".0", "")
                if row["heightcat"] not in ["", "-1"]
                else None
            )  # noqa
            parcel.baths = row["bathcat"] if row["bathcat"] != "-1" else None
            parcel.garage = row["has_garage"] == "1"
            parcel.exterior = (
                row["extcat"].replace(".0", "")
                if (row["extcat"] not in ["-1", ""])
                else None
            )
            db.session.merge(parcel)
        db.session.commit()
