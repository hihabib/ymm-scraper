from typing import Optional
from sqlalchemy.orm import Session
from sqlalchemy import desc
from datetime import datetime
from ..db.db import SessionLocal
from ..core.models import (
    TireRackYMM, 
    ScrapeErrorLog, 
    TireRackTireSize, 
    CustomWheelOffsetYMM, 
    CustomWheelOffsetData,
    DriverRightYMM,
    DriverRightVehicleSpec,
    DriverRightTireOption
)
import json

def insert_ymm(year: str, make: str, model: str, clarifier: Optional[str] = None) -> int:
    """Insert a YMM row and return its ID."""
    with SessionLocal() as session:  # type: Session
        obj = TireRackYMM(year=year, make=make, model=model, clarifier=clarifier)
        session.add(obj)
        session.commit()
        session.refresh(obj)
        return obj.id

def get_last_ymm() -> Optional[TireRackYMM]:
    """Return the most recently inserted YMM row (by created_at), or None."""
    with SessionLocal() as session:  # type: Session
        return (
            session.query(TireRackYMM)
            .order_by(TireRackYMM.created_at.desc(), TireRackYMM.id.desc())
            .first()
        )


def get_last_custom_wheel_offset_ymm() -> Optional[CustomWheelOffsetYMM]:
    """Return the most recently inserted CustomWheelOffset YMM row (by created_at), or None."""
    with SessionLocal() as session:  # type: Session
        return (
            session.query(CustomWheelOffsetYMM)
            .order_by(CustomWheelOffsetYMM.created_at.desc(), CustomWheelOffsetYMM.id.desc())
            .first()
        )


def check_custom_wheel_offset_combination_exists(year: str, make: str, model: str, trim: str, drive: str) -> bool:
    """Check if a custom wheel offset combination already exists in the database."""
    with SessionLocal() as session:  # type: Session
        existing = session.query(CustomWheelOffsetYMM).filter(
            CustomWheelOffsetYMM.year == year,
            CustomWheelOffsetYMM.make == make,
            CustomWheelOffsetYMM.model == model,
            CustomWheelOffsetYMM.trim == trim,
            CustomWheelOffsetYMM.drive == drive
        ).first()
        return existing is not None

def insert_error_log(source: str, context: dict, message: str) -> int:
    """Insert an error log entry and return its ID."""
    with SessionLocal() as session:  # type: Session
        obj = ScrapeErrorLog(
            source=source,
            context=json.dumps(context, ensure_ascii=False),
            message=message,
        )
        session.add(obj)
        session.commit()
        session.refresh(obj)
        return obj.id

def list_ymm(limit: int | None = None, offset: int = 0) -> list[TireRackYMM]:
    """List YMM rows ordered by id ascending."""
    with SessionLocal() as session:  # type: Session
        q = session.query(TireRackYMM).order_by(TireRackYMM.id.asc())
        if offset:
            q = q.offset(offset)
        if limit is not None:
            q = q.limit(limit)
        return list(q.all())



def insert_tire_sizes_for_ymm(ymm_id: int, original_sizes: list[dict], optional_sizes: list[dict]) -> int:
    """Insert tire sizes (original and optional) for a given YMM.

    Each item dict must have keys: 'front' and 'rear'. Returns count inserted.
    """
    total = 0
    with SessionLocal() as session:  # type: Session
        to_add: list[TireRackTireSize] = []
        for it in original_sizes or []:
            front = (it.get("front") or "").strip()
            rear = (it.get("rear") or "").strip()
            if not front and not rear:
                continue
            to_add.append(TireRackTireSize(ymm_id=ymm_id, category="original", front=front or "", rear=(rear or None)))
        for it in optional_sizes or []:
            front = (it.get("front") or "").strip()
            rear = (it.get("rear") or "").strip()
            if not front and not rear:
                continue
            to_add.append(TireRackTireSize(ymm_id=ymm_id, category="optional", front=front or "", rear=(rear or None)))
        if to_add:
            session.add_all(to_add)
            session.commit()
            total = len(to_add)
    return total


def insert_driver_right_ymm(year: str, make: str, model: str, body_type: str, 
                           sub_model: str, drd_model_id: str, drd_chassis_id: str) -> int:
    """Insert a DriverRight YMM row and return its ID."""
    with SessionLocal() as session:  # type: Session
        obj = DriverRightYMM(
            year=year,
            make=make,
            model=model,
            body_type=body_type,
            sub_model=sub_model,
            drd_model_id=drd_model_id,
            drd_chassis_id=drd_chassis_id
        )
        session.add(obj)
        session.commit()
        session.refresh(obj)
        return obj.id


def insert_driver_right_vehicle_spec(ymm_id: int, spec_data: dict) -> int:
    """Insert DriverRight vehicle specification data and return its ID."""
    with SessionLocal() as session:  # type: Session
        obj = DriverRightVehicleSpec(
            ymm_id=ymm_id,
            vehicle_length_inches=spec_data.get("VehicleLength_Inches"),
            vehicle_width_inches=spec_data.get("VehicleWidth_Inches"),
            vehicle_height_inches=spec_data.get("VehicleHeight_Inches"),
            wheelbase_inches=spec_data.get("Wheelbase_Inches"),
            vehicle_track_f_inches=spec_data.get("VehicleTrack_F_Inches"),
            vehicle_track_r_inches=spec_data.get("VehicleTrack_R_Inches"),
            gvw_lbs=spec_data.get("GVW_Lbs"),
            axle_weight_f_lbs=spec_data.get("Axle_Weight_F_Lbs"),
            axle_weight_r_lbs=spec_data.get("Axle_Weight_R_Lbs"),
            rim_width_max_f=spec_data.get("RimWidth_Max_F"),
            rim_width_max_r=spec_data.get("RimWidth_Max_R"),
            center_bore_r=spec_data.get("CenterBore_R"),
            tpms=spec_data.get("TPMS"),
            nut_or_bolt=spec_data.get("NutorBolt"),
            suv_car=spec_data.get("SUV_Car"),
            min_bolt_length_min_inches=spec_data.get("MinBoltLength_Min_Inches"),
            max_bolt_length_max_inches=spec_data.get("MaxBoltLength__Max_Inches"),
            nut_bolt_am=spec_data.get("NutBoltAM"),
            nut_bolt_am_length_inches=spec_data.get("NutBoltAMLength_Inches"),
            nut_bolt_oe_alloy=spec_data.get("NutBoltOEAlloy"),
            nut_bolt_oe_alloy_length_inches=spec_data.get("NutBoltOEAlloyLength_Inches"),
            nut_bolt_oe_steel=spec_data.get("NutBoltOESteel"),
            nut_bolt_oe_steel_length_inches=spec_data.get("NutBoltOESteelLength_Inches"),
            oe_tire_description=spec_data.get("OETireDescription"),
            caliper=spec_data.get("Caliper"),
            change_date_us=spec_data.get("ChangeDate_US")
        )
        session.add(obj)
        session.commit()
        session.refresh(obj)
        return obj.id


def insert_driver_right_tire_options(ymm_id: int, primary_option: dict, options: list) -> int:
    """Insert DriverRight tire options (primary and additional options) and return count inserted."""
    total = 0
    with SessionLocal() as session:  # type: Session
        to_add: list[DriverRightTireOption] = []
        
        # Insert primary option
        if primary_option:
            to_add.append(DriverRightTireOption(
                ymm_id=ymm_id,
                option_type="primary",
                model_name=primary_option.get("ModelName"),
                horse_power=primary_option.get("HorsePower"),
                vin=primary_option.get("VIN"),
                uk_year=primary_option.get("UKYear"),
                tire_size=primary_option.get("TireSize"),
                load_index=primary_option.get("LoadIndex"),
                speed_index=primary_option.get("SpeedIndex"),
                tire_pressure_psi=primary_option.get("TirePressure_PSI"),
                rim_size=primary_option.get("RimSize"),
                rim_offset=primary_option.get("RimOffset"),
                run_flat_f=primary_option.get("RunFlat_F"),
                extra_load_f=primary_option.get("ExtraLoad_F"),
                tire_size_r=primary_option.get("TireSize_R"),
                load_index_r=primary_option.get("LoadIndex_R"),
                speed_index_r=primary_option.get("SpeedIndex_R"),
                tire_pressure_r_psi=primary_option.get("TirePressure_R_PSI"),
                rim_size_r=primary_option.get("RimSize_R"),
                offset_r=primary_option.get("Offset_R"),
                run_flat_r=primary_option.get("RunFlat_R"),
                extra_load_r=primary_option.get("ExtraLoad_R"),
                model_laden_tp_f_psi=primary_option.get("Model_Laden_TP_F_PSI"),
                model_laden_tp_r_psi=primary_option.get("Model_Laden_TP_R_PSI"),
                oe_description=primary_option.get("OEDescription"),
                change_date_us=primary_option.get("ChangeDate_US")
            ))
        
        # Insert additional options
        for option in options or []:
            to_add.append(DriverRightTireOption(
                ymm_id=ymm_id,
                option_type="option",
                model_name=option.get("ModelName"),
                horse_power=option.get("HorsePower"),
                vin=option.get("VIN"),
                uk_year=option.get("UKYear"),
                tire_size=option.get("TireSize"),
                load_index=option.get("LoadIndex"),
                speed_index=option.get("SpeedIndex"),
                tire_pressure_psi=option.get("TirePressure_PSI"),
                rim_size=option.get("RimSize"),
                rim_offset=option.get("RimOffset"),
                run_flat_f=option.get("RunFlat_F"),
                extra_load_f=option.get("ExtraLoad_F"),
                tire_size_r=option.get("TireSize_R"),
                load_index_r=option.get("LoadIndex_R"),
                speed_index_r=option.get("SpeedIndex_R"),
                tire_pressure_r_psi=option.get("TirePressure_R_PSI"),
                rim_size_r=option.get("RimSize_R"),
                offset_r=option.get("Offset_R"),
                run_flat_r=option.get("RunFlat_R"),
                extra_load_r=option.get("ExtraLoad_R"),
                model_laden_tp_f_psi=option.get("Model_Laden_TP_F_PSI"),
                model_laden_tp_r_psi=option.get("Model_Laden_TP_R_PSI"),
                oe_description=option.get("OEDescription"),
                change_date_us=option.get("ChangeDate_US")
            ))
        
        if to_add:
            session.add_all(to_add)
            session.commit()
            total = len(to_add)
    
    return total


def get_last_driver_right_ymm() -> Optional[DriverRightYMM]:
    """Return the most recently inserted DriverRight YMM row (by created_at), or None."""
    with SessionLocal() as session:  # type: Session
        return (
            session.query(DriverRightYMM)
            .order_by(DriverRightYMM.created_at.desc(), DriverRightYMM.id.desc())
            .first()
        )


def insert_custom_wheel_offset_ymm(year: str, make: str, model: str, trim: str, drive: str, 
                                   vehicle_type: str, dr_chassis_id: str, suspension: str = None, 
                                   modification: str = None, rubbing: str = None, bolt_pattern: str = None) -> int:
    """Insert a CustomWheelOffset YMM row and return its ID."""
    with SessionLocal() as session:  # type: Session
        obj = CustomWheelOffsetYMM(
            year=year, 
            make=make, 
            model=model, 
            trim=trim, 
            drive=drive,
            vehicle_type=vehicle_type,
            dr_chassis_id=dr_chassis_id,
            suspension=suspension,
            modification=modification,
            rubbing=rubbing,
            bolt_pattern=bolt_pattern
        )
        session.add(obj)
        session.commit()
        session.refresh(obj)
        return obj.id


def insert_custom_wheel_offset_data(ymm_id: int, parsed_data: dict) -> int:
    """
    Insert CustomWheelOffset data for both front and rear positions.
    
    Args:
        ymm_id: The ID from custom_wheel_offset_ymm table
        parsed_data: Dict with structure like:
            {
                'front': {'diameter': {'min': 19, 'max': 24}, 'width': {'min': 8.5, 'max': 10.0}, 'offset': {'min': 35, 'max': 60}},
                'rear': {'diameter': {'min': 19, 'max': 24}, 'width': {'min': 8.5, 'max': 10.0}, 'offset': {'min': 35, 'max': 60}}
            }
    
    Returns:
        Number of records inserted (typically 2: front and rear)
    """
    total = 0
    with SessionLocal() as session:  # type: Session
        to_add: list[CustomWheelOffsetData] = []
        
        for position in ['front', 'rear']:
            if position in parsed_data:
                pos_data = parsed_data[position]
                diameter = pos_data.get('diameter', {})
                width = pos_data.get('width', {})
                offset = pos_data.get('offset', {})
                
                # Ensure all required data is present
                if (diameter.get('min') is not None and diameter.get('max') is not None and
                    width.get('min') is not None and width.get('max') is not None and
                    offset.get('min') is not None and offset.get('max') is not None):
                    
                    to_add.append(CustomWheelOffsetData(
                        ymm_id=ymm_id,
                        position=position,
                        diameter_min=int(diameter['min']),
                        diameter_max=int(diameter['max']),
                        width_min=str(width['min']),  # Store as string to preserve precision
                        width_max=str(width['max']),  # Store as string to preserve precision
                        offset_min=int(offset['min']),
                        offset_max=int(offset['max'])
                    ))
        
        if to_add:
            session.add_all(to_add)
            session.commit()
            total = len(to_add)
    
    return total