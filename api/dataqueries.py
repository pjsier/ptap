import pandas as pd
from geoalchemy2.functions import ST_DistanceSphere
from sqlalchemy.sql import func

from .db import db
from .models import CookParcel, DetroitParcel, MilwaukeeParcel


# TODO: autocomplete/typeahead?
# sql query things
def address_candidates_query(region, st_num):
    model = None
    if region == "cook":
        model = CookParcel
    elif region == "detroit":
        model = DetroitParcel
    elif region == "milwaukee":
        model = MilwaukeeParcel
    return model.query.filter(model.street_number == st_num.strip())


def _get_pin(region, pin):
    if region == "cook":
        model = CookParcel
    elif region == "detroit":
        model = DetroitParcel
    elif region == "milwaukee":
        model = MilwaukeeParcel
    return model.query.filter(model.pin == pin).first()


def _get_pin_with_distance(region, pin, parcel):
    if region == "cook":
        model = CookParcel
    elif region == "detroit":
        model = DetroitParcel
    elif region == "milwaukee":
        model = MilwaukeeParcel
    result = (
        db.session.query(
            model, ST_DistanceSphere(model.geom, parcel.geom).label("distance")
        )
        .filter(model.pin == pin)
        .first()
    )
    if result is None:
        return (None, None)
    return result


def get_pin(region, pin):
    data_list = []
    parcel = _get_pin(region, pin)
    if parcel:
        data_list = [parcel.as_dict()]
    return pd.DataFrame(data_list)


def avg_ecf(neighborhood):
    return (
        db.session.query(func.avg(DetroitParcel.sale_price / DetroitParcel.total_sq_ft))
        .filter(
            DetroitParcel.sale_price.isnot(None),
            DetroitParcel.total_sq_ft.isnot(None),
            DetroitParcel.total_sq_ft > 0,
            DetroitParcel.neighborhood == neighborhood,
        )
        .scalar()
    )
