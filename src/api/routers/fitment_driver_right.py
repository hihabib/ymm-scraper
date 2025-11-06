from fastapi import APIRouter, Query
from typing import Optional

from src.db.db import SessionLocal
from src.core.models import DriverRightYMM, DriverRightVehicleSpec, DriverRightTireOption
from ..utils.response import success, error_json


router = APIRouter(prefix="/fitment/driver-right", tags=["fitment"])


def _rows_to_list(rows):
    return [r[0] for r in rows if r and r[0] is not None]


def _serialize_model(obj):
    cols = obj.__table__.columns.keys()
    return {c: getattr(obj, c) for c in cols}


@router.get("/get-years")
def get_years():
    """Return unique years from driver_right_ymm, latest first."""
    try:
        with SessionLocal() as session:
            rows = (
                session.query(DriverRightYMM.year)
                .distinct()
                .order_by(DriverRightYMM.year.desc())
                .all()
            )
            years = _rows_to_list(rows)
            return success(data=years, message="Years fetched", status_code=200)
    except Exception as e:
        return error_json(message=f"Failed to fetch years: {e}", status_code=500)


@router.get("/get-makes")
def get_makes(year: str = Query(..., description="Filter by year")):
    """Return unique makes for a given year from driver_right_ymm, ascending alphabetically."""
    try:
        with SessionLocal() as session:
            rows = (
                session.query(DriverRightYMM.make)
                .filter(DriverRightYMM.year == year)
                .distinct()
                .order_by(DriverRightYMM.make.asc())
                .all()
            )
            makes = _rows_to_list(rows)
            return success(data=makes, message="Makes fetched", status_code=200)
    except Exception as e:
        return error_json(message=f"Failed to fetch makes: {e}", status_code=500)


@router.get("/get-models")
def get_models(
    year: str = Query(..., description="Filter by year"),
    make: str = Query(..., description="Filter by make"),
):
    """Return unique models for given year and make from driver_right_ymm, ascending alphabetically."""
    try:
        with SessionLocal() as session:
            rows = (
                session.query(DriverRightYMM.model)
                .filter(DriverRightYMM.year == year, DriverRightYMM.make == make)
                .distinct()
                .order_by(DriverRightYMM.model.asc())
                .all()
            )
            models = _rows_to_list(rows)
            return success(data=models, message="Models fetched", status_code=200)
    except Exception as e:
        return error_json(message=f"Failed to fetch models: {e}", status_code=500)


@router.get("/get-body-types")
def get_body_types(
    year: str = Query(..., description="Filter by year"),
    make: str = Query(..., description="Filter by make"),
    model: str = Query(..., description="Filter by model"),
):
    """Return unique body types for given year, make, model from driver_right_ymm, ascending alphabetically."""
    try:
        with SessionLocal() as session:
            rows = (
                session.query(DriverRightYMM.body_type)
                .filter(
                    DriverRightYMM.year == year,
                    DriverRightYMM.make == make,
                    DriverRightYMM.model == model,
                )
                .distinct()
                .order_by(DriverRightYMM.body_type.asc())
                .all()
            )
            body_types = _rows_to_list(rows)
            return success(data=body_types, message="Body types fetched", status_code=200)
    except Exception as e:
        return error_json(message=f"Failed to fetch body types: {e}", status_code=500)


@router.get("/get-sub-models")
def get_sub_models(
    year: str = Query(..., description="Filter by year"),
    make: str = Query(..., description="Filter by make"),
    model: str = Query(..., description="Filter by model"),
    body_type: str = Query(..., alias="body-type", description="Filter by body type"),
):
    """Return unique sub models for given year, make, model, body type from driver_right_ymm, ascending alphabetically."""
    try:
        with SessionLocal() as session:
            rows = (
                session.query(DriverRightYMM.sub_model)
                .filter(
                    DriverRightYMM.year == year,
                    DriverRightYMM.make == make,
                    DriverRightYMM.model == model,
                    DriverRightYMM.body_type == body_type,
                )
                .distinct()
                .order_by(DriverRightYMM.sub_model.asc())
                .all()
            )
            sub_models = _rows_to_list(rows)
            return success(data=sub_models, message="Sub models fetched", status_code=200)
    except Exception as e:
        return error_json(message=f"Failed to fetch sub models: {e}", status_code=500)


@router.get("/get-vehicle-info")
def get_vehicle_info(
    year: str = Query(..., description="Filter by year"),
    make: str = Query(..., description="Filter by make"),
    model: str = Query(..., description="Filter by model"),
    body_type: str = Query(..., alias="body-type", description="Filter by body type"),
    sub_model: str = Query(..., alias="sub-model", description="Filter by sub model"),
):
    """Return vehicle info, including YMM, tire options, and vehicle specs for the combination."""
    try:
        with SessionLocal() as session:
            ymm = (
                session.query(DriverRightYMM)
                .filter(
                    DriverRightYMM.year == year,
                    DriverRightYMM.make == make,
                    DriverRightYMM.model == model,
                    DriverRightYMM.body_type == body_type,
                    DriverRightYMM.sub_model == sub_model,
                )
                .order_by(DriverRightYMM.created_at.desc(), DriverRightYMM.id.desc())
                .first()
            )

            if not ymm:
                return error_json(message="Vehicle combination not found", status_code=404)

            specs = (
                session.query(DriverRightVehicleSpec)
                .filter(DriverRightVehicleSpec.ymm_id == ymm.id)
                .all()
            )

            options = (
                session.query(DriverRightTireOption)
                .filter(DriverRightTireOption.ymm_id == ymm.id)
                .all()
            )

            data = {
                "year": ymm.year,
                "make": ymm.make,
                "model": ymm.model,
                "bodyType": ymm.body_type,
                "subModel": ymm.sub_model,
                "drdModelId": ymm.drd_model_id,
                "drdChassisId": ymm.drd_chassis_id,
                "vehicleSpecs": [_serialize_model(s) for s in specs],
                "tireOptions": [_serialize_model(o) for o in options],
            }

            return success(data=data, message="Vehicle info fetched", status_code=200)
    except Exception as e:
        return error_json(message=f"Failed to fetch vehicle info: {e}", status_code=500)