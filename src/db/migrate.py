"""Simple migration helpers.

Creates tables if they don't exist and applies minimal, safe schema changes
that SQLAlchemy's `create_all` doesn't handle (e.g., adding a column).
"""
# Ensure 'src' is on sys.path when run directly (e.g., python src/db/migrate.py)
import sys
from pathlib import Path
SRC_DIR = Path(__file__).resolve().parents[1]
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from sqlalchemy import inspect, text
try:
    from db.db import engine
    from core.models import Base
except ImportError:
    # Allow running this file directly without PYTHONPATH set
    import sys
    from pathlib import Path
    SRC_DIR = Path(__file__).resolve().parents[1]
    if str(SRC_DIR) not in sys.path:
        sys.path.insert(0, str(SRC_DIR))
    from db.db import engine
    from core.models import Base


def run_migrations() -> None:
    """Run minimal migrations: create tables then add missing columns."""
    # Create tables from metadata (does NOT add columns to existing tables)
    Base.metadata.create_all(bind=engine)

    # Add missing created_at column to tirerack_ymm if it's absent
    insp = inspect(engine)
    table_name = "tirerack_ymm"
    if table_name in insp.get_table_names():
        cols = [c["name"] for c in insp.get_columns(table_name)]
        if "created_at" not in cols:
            # Choose dialect-specific DDL
            if engine.dialect.name == "postgresql":
                ddl = (
                    "ALTER TABLE tirerack_ymm "
                    "ADD COLUMN created_at TIMESTAMPTZ NOT NULL DEFAULT now();"
                )
            elif engine.dialect.name == "sqlite":
                ddl = (
                    "ALTER TABLE tirerack_ymm "
                    "ADD COLUMN created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP;"
                )
            else:
                ddl = (
                    "ALTER TABLE tirerack_ymm "
                    "ADD COLUMN created_at TIMESTAMP;"
                )
            with engine.begin() as conn:
                conn.execute(text(ddl))

    # Ensure scrape_error_log table exists
    err_table = "scrape_error_log"
    if err_table not in insp.get_table_names():
        # Base.metadata.create_all should have created it, but if not, attempt explicit creation
        Base.metadata.create_all(bind=engine)



    # Ensure driver_right tables exist
    driver_right_tables = ["driver_right_ymm", "driver_right_vehicle_specs", "driver_right_tire_options"]
    for table_name in driver_right_tables:
        if table_name not in insp.get_table_names():
            Base.metadata.create_all(bind=engine)
            break  # Only need to call create_all once if any table is missing

    # Add missing processed column to custom_wheel_offset_ymm if it's absent
    cwo_ymm_table = "custom_wheel_offset_ymm"
    if cwo_ymm_table in insp.get_table_names():
        cols = [c["name"] for c in insp.get_columns(cwo_ymm_table)]
        if "processed" not in cols:
            # Choose dialect-specific DDL
            if engine.dialect.name == "postgresql":
                ddl = (
                    "ALTER TABLE custom_wheel_offset_ymm "
                    "ADD COLUMN processed INTEGER NOT NULL DEFAULT 0;"
                )
            elif engine.dialect.name == "sqlite":
                ddl = (
                    "ALTER TABLE custom_wheel_offset_ymm "
                    "ADD COLUMN processed INTEGER NOT NULL DEFAULT 0;"
                )
            else:
                ddl = (
                    "ALTER TABLE custom_wheel_offset_ymm "
                    "ADD COLUMN processed INTEGER NOT NULL DEFAULT 0;"
                )
            with engine.begin() as conn:
                conn.execute(text(ddl))

    # Add missing bolt_pattern column to custom_wheel_offset_ymm if it's absent
    cwo_ymm_table = "custom_wheel_offset_ymm"
    if cwo_ymm_table in insp.get_table_names():
        cols = [c["name"] for c in insp.get_columns(cwo_ymm_table)]
        if "bolt_pattern" not in cols:
            # Choose dialect-specific DDL
            if engine.dialect.name == "postgresql":
                ddl = (
                    "ALTER TABLE custom_wheel_offset_ymm "
                    "ADD COLUMN bolt_pattern VARCHAR(50);"
                )
            elif engine.dialect.name == "sqlite":
                ddl = (
                    "ALTER TABLE custom_wheel_offset_ymm "
                    "ADD COLUMN bolt_pattern VARCHAR(50);"
                )
            else:
                ddl = (
                    "ALTER TABLE custom_wheel_offset_ymm "
                    "ADD COLUMN bolt_pattern VARCHAR(50);"
                )
            with engine.begin() as conn:
                conn.execute(text(ddl))

    # Make custom_wheel_offset_ymm columns nullable (for omitted data validation)
    cwo_ymm_table = "custom_wheel_offset_ymm"
    if cwo_ymm_table in insp.get_table_names():
        # Columns that should be nullable since data validation is omitted
        nullable_columns = ["year", "make", "model", "trim", "drive", "vehicle_type", "dr_chassis_id"]
        
        for col_name in nullable_columns:
            try:
                if engine.dialect.name == "postgresql":
                    ddl = f"ALTER TABLE {cwo_ymm_table} ALTER COLUMN {col_name} DROP NOT NULL;"
                elif engine.dialect.name == "sqlite":
                    # SQLite doesn't support ALTER COLUMN directly for nullable changes
                    # The model change will handle this for new tables
                    continue
                else:
                    # MySQL and others
                    # Get the column type first
                    cols = {c["name"]: c for c in insp.get_columns(cwo_ymm_table)}
                    if col_name in cols:
                        col_type = str(cols[col_name]["type"])
                        ddl = f"ALTER TABLE {cwo_ymm_table} MODIFY COLUMN {col_name} {col_type} NULL;"
                    else:
                        continue
                
                with engine.begin() as conn:
                    conn.execute(text(ddl))
                    
            except Exception as e:
                # Column might already be nullable or other issues - continue with other columns
                print(f"Warning: Could not make column {col_name} nullable: {e}")

    # Remove bolt_pattern column from custom_wheel_offset_data if it exists (correcting previous mistake)
    cwo_data_table = "custom_wheel_offset_data"
    if cwo_data_table in insp.get_table_names():
        cols = [c["name"] for c in insp.get_columns(cwo_data_table)]
        if "bolt_pattern" in cols:
            # Choose dialect-specific DDL to drop column
            if engine.dialect.name == "postgresql":
                ddl = (
                    "ALTER TABLE custom_wheel_offset_data "
                    "DROP COLUMN bolt_pattern;"
                )
            elif engine.dialect.name == "sqlite":
                # SQLite doesn't support DROP COLUMN directly, but we can skip this
                # since the model change will handle it for new tables
                ddl = None
            else:
                ddl = (
                    "ALTER TABLE custom_wheel_offset_data "
                    "DROP COLUMN bolt_pattern;"
                )
            if ddl:
                with engine.begin() as conn:
                    conn.execute(text(ddl))

    # Update custom_wheel_offset_data column types to string for diameter, width, and offset fields
    cwo_data_table = "custom_wheel_offset_data"
    if cwo_data_table in insp.get_table_names():
        cols = {c["name"]: c for c in insp.get_columns(cwo_data_table)}
        
        # Check if we need to update column types from integer/float to string
        columns_to_update = [
            ("diameter_min", "VARCHAR(20)"),
            ("diameter_max", "VARCHAR(20)"),
            ("width_min", "VARCHAR(20)"),
            ("width_max", "VARCHAR(20)"),
            ("offset_min", "VARCHAR(20)"),
            ("offset_max", "VARCHAR(20)")
        ]
        
        for col_name, new_type in columns_to_update:
            if col_name in cols:
                current_type = str(cols[col_name]["type"]).upper()
                # Only update if current type is not already VARCHAR/TEXT
                if "VARCHAR" not in current_type and "TEXT" not in current_type:
                    if engine.dialect.name == "postgresql":
                        ddl = f"ALTER TABLE {cwo_data_table} ALTER COLUMN {col_name} TYPE {new_type};"
                    elif engine.dialect.name == "sqlite":
                        # SQLite doesn't support ALTER COLUMN TYPE directly
                        # For SQLite, we'll need to recreate the table or handle this differently
                        # For now, we'll skip this for SQLite as the model change will handle new tables
                        continue
                    else:
                        ddl = f"ALTER TABLE {cwo_data_table} MODIFY COLUMN {col_name} {new_type};"
                    
                    try:
                        with engine.begin() as conn:
                            conn.execute(text(ddl))
                    except Exception as e:
                        print(f"Warning: Could not update column {col_name} type: {e}")

    # Make custom_wheel_offset_data wheel specification columns nullable (for omitted data validation)
    cwo_data_table = "custom_wheel_offset_data"
    if cwo_data_table in insp.get_table_names():
        # Columns that should be nullable since data validation is omitted
        nullable_columns = ["diameter_min", "diameter_max", "width_min", "width_max", "offset_min", "offset_max"]
        
        for col_name in nullable_columns:
            try:
                if engine.dialect.name == "postgresql":
                    ddl = f"ALTER TABLE {cwo_data_table} ALTER COLUMN {col_name} DROP NOT NULL;"
                elif engine.dialect.name == "sqlite":
                    # SQLite doesn't support ALTER COLUMN directly for nullable changes
                    # The model change will handle this for new tables
                    continue
                else:
                    # MySQL and others
                    # Get the column type first
                    cols = {c["name"]: c for c in insp.get_columns(cwo_data_table)}
                    if col_name in cols:
                        col_type = str(cols[col_name]["type"])
                        ddl = f"ALTER TABLE {cwo_data_table} MODIFY COLUMN {col_name} {col_type} NULL;"
                    else:
                        continue
                
                with engine.begin() as conn:
                    conn.execute(text(ddl))
                    
            except Exception as e:
                # Column might already be nullable or other issues - continue with other columns
                print(f"Warning: Could not make column {col_name} nullable: {e}")

    # After creating/aligning schemas, optionally clean up unused tables
    drop_unused_tables()


def drop_unused_tables() -> None:
    """Drop tables that are not defined in our ORM and match app prefixes.

    Safety:
    - Only drops tables whose names start with managed prefixes ("tirerack_", "scrape_").
    - Never touches non-app tables.
    - Prints a report of findings and actions.
    """
    insp = inspect(engine)
    existing = set(insp.get_table_names())
    # Tables we intentionally manage via SQLAlchemy models
    expected = {
        "tirerack_ymm",
        "scrape_error_log",
        "tirerack_tire_sizes",
        "custom_wheel_offset_ymm",
        "custom_wheel_offset_data",
    }
    managed_prefixes = ("tirerack_", "scrape_", "custom_wheel_offset_")

    # Candidates are app-namespaced tables not present in expected set
    candidates = sorted([
        t for t in existing
        if t not in expected and any(t.startswith(p) for p in managed_prefixes)
    ])

    if not candidates:
        print("[migrate] No unused app tables found.")
        return

    print("[migrate] Unused app tables detected (will drop):", ", ".join(candidates))

    # Dialect-specific DROP statements
    for t in candidates:
        try:
            if engine.dialect.name == "postgresql":
                ddl = f'DROP TABLE IF EXISTS "{t}" CASCADE;'
            else:
                # SQLite and others
                ddl = f"DROP TABLE IF EXISTS {t};"
            with engine.begin() as conn:
                conn.execute(text(ddl))
            print(f"[migrate] Dropped table: {t}")
        except Exception as e:
            print(f"[migrate] Failed to drop table {t}: {e}")


if __name__ == "__main__":
    run_migrations()