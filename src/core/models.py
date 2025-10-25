from sqlalchemy.orm import declarative_base, Mapped, mapped_column
from sqlalchemy import Integer, String, DateTime, func, Text, ForeignKey

Base = declarative_base()

class TireRackYMM(Base):
    __tablename__ = "tirerack_ymm"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    year: Mapped[str] = mapped_column(String(10), nullable=False)
    make: Mapped[str] = mapped_column(String(100), nullable=False)
    model: Mapped[str] = mapped_column(String(150), nullable=False)
    clarifier: Mapped[str] = mapped_column(String(200), nullable=True)
    created_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)

__all__ = ["Base", "TireRackYMM"]
class ScrapeErrorLog(Base):
    __tablename__ = "scrape_error_log"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    source: Mapped[str] = mapped_column(String(100), nullable=False)  # e.g., "tire_rack"
    context: Mapped[str] = mapped_column(Text, nullable=False)  # serialized context (make/year/model/clarifier)
    message: Mapped[str] = mapped_column(Text, nullable=False)  # error message/exception text
    created_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)



class TireRackTireSize(Base):
    __tablename__ = "tirerack_tire_sizes"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    ymm_id: Mapped[int] = mapped_column(Integer, ForeignKey("tirerack_ymm.id"), nullable=False)
    category: Mapped[str] = mapped_column(String(20), nullable=False)  # "original" or "optional"
    front: Mapped[str] = mapped_column(String(100), nullable=False)
    rear: Mapped[str] = mapped_column(String(100), nullable=True)
    created_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class CustomWheelOffsetYMM(Base):
    __tablename__ = "custom_wheel_offset_ymm"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    year: Mapped[str] = mapped_column(String(10), nullable=True)
    make: Mapped[str] = mapped_column(String(100), nullable=True)
    model: Mapped[str] = mapped_column(String(150), nullable=True)
    trim: Mapped[str] = mapped_column(String(200), nullable=True)
    drive: Mapped[str] = mapped_column(String(50), nullable=True)
    vehicle_type: Mapped[str] = mapped_column(String(50), nullable=True)
    dr_chassis_id: Mapped[str] = mapped_column(String(100), nullable=True)
    suspension: Mapped[str] = mapped_column(String(100), nullable=True)
    modification: Mapped[str] = mapped_column(String(100), nullable=True)
    rubbing: Mapped[str] = mapped_column(String(100), nullable=True)
    bolt_pattern: Mapped[str] = mapped_column(String(50), nullable=True)  # Store bolt pattern like "5x120mm (5x4.72")"
    processed: Mapped[bool] = mapped_column(Integer, nullable=False, default=0)  # 0 = not processed, 1 = processed
    created_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class CustomWheelOffsetData(Base):
    __tablename__ = "custom_wheel_offset_data"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    ymm_id: Mapped[int] = mapped_column(Integer, ForeignKey("custom_wheel_offset_ymm.id"), nullable=False)
    position: Mapped[str] = mapped_column(String(10), nullable=False)  # "front" or "rear"
    diameter_min: Mapped[str] = mapped_column(String(20), nullable=True)  # Store with units like "19\""
    diameter_max: Mapped[str] = mapped_column(String(20), nullable=True)  # Store with units like "22\""
    width_min: Mapped[str] = mapped_column(String(20), nullable=True)  # Store with units like "7.5\""
    width_max: Mapped[str] = mapped_column(String(20), nullable=True)  # Store with units like "9.5\""
    offset_min: Mapped[str] = mapped_column(String(20), nullable=True)  # Store with units like "46mm"
    offset_max: Mapped[str] = mapped_column(String(20), nullable=True)  # Store with units like "60mm"
    created_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)

class DriverRightYMM(Base):
    __tablename__ = "driver_right_ymm"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    year: Mapped[str] = mapped_column(String(10), nullable=False)
    make: Mapped[str] = mapped_column(String(100), nullable=False)
    model: Mapped[str] = mapped_column(String(150), nullable=False)
    body_type: Mapped[str] = mapped_column(String(100), nullable=False)
    sub_model: Mapped[str] = mapped_column(String(200), nullable=False)
    drd_model_id: Mapped[str] = mapped_column(String(50), nullable=False)
    drd_chassis_id: Mapped[str] = mapped_column(String(50), nullable=False)
    created_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class DriverRightVehicleSpec(Base):
    __tablename__ = "driver_right_vehicle_specs"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    ymm_id: Mapped[int] = mapped_column(Integer, ForeignKey("driver_right_ymm.id"), nullable=False)
    
    # Vehicle Dimensions
    vehicle_length_inches: Mapped[str] = mapped_column(String(20), nullable=True)
    vehicle_width_inches: Mapped[str] = mapped_column(String(20), nullable=True)
    vehicle_height_inches: Mapped[str] = mapped_column(String(20), nullable=True)
    wheelbase_inches: Mapped[str] = mapped_column(String(20), nullable=True)
    vehicle_track_f_inches: Mapped[str] = mapped_column(String(20), nullable=True)
    vehicle_track_r_inches: Mapped[str] = mapped_column(String(20), nullable=True)
    
    # Weight Information
    gvw_lbs: Mapped[str] = mapped_column(String(20), nullable=True)
    axle_weight_f_lbs: Mapped[str] = mapped_column(String(20), nullable=True)
    axle_weight_r_lbs: Mapped[str] = mapped_column(String(20), nullable=True)
    
    # Wheel/Rim Specifications
    rim_width_max_f: Mapped[str] = mapped_column(String(20), nullable=True)
    rim_width_max_r: Mapped[str] = mapped_column(String(20), nullable=True)
    center_bore_r: Mapped[str] = mapped_column(String(20), nullable=True)
    
    # System Information
    tpms: Mapped[str] = mapped_column(String(20), nullable=True)
    nut_or_bolt: Mapped[str] = mapped_column(String(20), nullable=True)
    suv_car: Mapped[str] = mapped_column(String(20), nullable=True)
    
    # Bolt/Nut Information
    min_bolt_length_min_inches: Mapped[str] = mapped_column(String(20), nullable=True)
    max_bolt_length_max_inches: Mapped[str] = mapped_column(String(20), nullable=True)
    nut_bolt_am: Mapped[str] = mapped_column(String(50), nullable=True)
    nut_bolt_am_length_inches: Mapped[str] = mapped_column(String(20), nullable=True)
    nut_bolt_oe_alloy: Mapped[str] = mapped_column(String(50), nullable=True)
    nut_bolt_oe_alloy_length_inches: Mapped[str] = mapped_column(String(20), nullable=True)
    nut_bolt_oe_steel: Mapped[str] = mapped_column(String(50), nullable=True)
    nut_bolt_oe_steel_length_inches: Mapped[str] = mapped_column(String(20), nullable=True)
    
    # Other Fields
    oe_tire_description: Mapped[str] = mapped_column(Text, nullable=True)
    caliper: Mapped[str] = mapped_column(String(20), nullable=True)
    change_date_us: Mapped[str] = mapped_column(String(50), nullable=True)
    
    created_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)


class DriverRightTireOption(Base):
    __tablename__ = "driver_right_tire_options"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    ymm_id: Mapped[int] = mapped_column(Integer, ForeignKey("driver_right_ymm.id"), nullable=False)
    option_type: Mapped[str] = mapped_column(String(20), nullable=False)  # "primary" or "option"
    
    # Vehicle Information
    model_name: Mapped[str] = mapped_column(String(300), nullable=True)
    horse_power: Mapped[str] = mapped_column(String(20), nullable=True)
    vin: Mapped[str] = mapped_column(String(100), nullable=True)
    uk_year: Mapped[str] = mapped_column(String(20), nullable=True)
    
    # Front Tire Information
    tire_size: Mapped[str] = mapped_column(String(50), nullable=True)
    load_index: Mapped[str] = mapped_column(String(20), nullable=True)
    speed_index: Mapped[str] = mapped_column(String(20), nullable=True)
    tire_pressure_psi: Mapped[str] = mapped_column(String(20), nullable=True)
    rim_size: Mapped[str] = mapped_column(String(50), nullable=True)
    rim_offset: Mapped[str] = mapped_column(String(20), nullable=True)
    run_flat_f: Mapped[str] = mapped_column(String(20), nullable=True)
    extra_load_f: Mapped[str] = mapped_column(String(20), nullable=True)
    
    # Rear Tire Information
    tire_size_r: Mapped[str] = mapped_column(String(50), nullable=True)
    load_index_r: Mapped[str] = mapped_column(String(20), nullable=True)
    speed_index_r: Mapped[str] = mapped_column(String(20), nullable=True)
    tire_pressure_r_psi: Mapped[str] = mapped_column(String(20), nullable=True)
    rim_size_r: Mapped[str] = mapped_column(String(50), nullable=True)
    offset_r: Mapped[str] = mapped_column(String(20), nullable=True)
    run_flat_r: Mapped[str] = mapped_column(String(20), nullable=True)
    extra_load_r: Mapped[str] = mapped_column(String(20), nullable=True)
    
    # Pressure Information
    model_laden_tp_f_psi: Mapped[str] = mapped_column(String(20), nullable=True)
    model_laden_tp_r_psi: Mapped[str] = mapped_column(String(20), nullable=True)
    
    # Other Information
    oe_description: Mapped[str] = mapped_column(Text, nullable=True)
    change_date_us: Mapped[str] = mapped_column(String(50), nullable=True)
    
    created_at: Mapped[DateTime] = mapped_column(DateTime(timezone=True), server_default=func.now(), nullable=False)


__all__ = [
    "Base",
    "TireRackYMM",
    "ScrapeErrorLog",
    "TireRackTireSize",
    "CustomWheelOffsetYMM",
    "CustomWheelOffsetData",
    "DriverRightYMM",
    "DriverRightVehicleSpec",
    "DriverRightTireOption",
]