import dagster as dg
import ibis
from pathlib import Path

from ..resources import ImpalaResource 

from utils.utils_bronze import materialize_bronze_asset, export_bronze_metadata

DATABASE_TARGET = "YOUR_DB_NAME"
CONTRACTS_DIR = Path("Your_Path")

# ---------------------------------------------------------
# --- BRONZE LAYER (Metadata Template) ---
# ---------------------------------------------------------

# 1. THE "PARENT" ASSET (Database)
@dg.asset(
    name="database_raw_asset",
    group_name="BRONZE",
    description="TEMPLATE: Connection and validation of database [DB_NAME]",
    metadata={ # Metadata Dataset level
        "source_system": "SOURCE_SYSTEM_DESCRIPTION", # <--- FILL IN
        "clinical_coverage": "", 
        "temporal_coverage": "",
        "standard_mapping": dg.MetadataValue.json({
            "target_standard": ""
        })
}
)
def database_raw_asset(context: dg.AssetExecutionContext, impala: ImpalaResource) -> str:
"""Validates the existence of the database and triggers the JSON metadata export."""    
    conn = impala.get_connection()
    db_name = "YOUR_DB_NAME" # <--- FILL IN
    tables = conn.list_tables(database=db_name)
    context.log.info(f"Database: {DATABASE_TARGET}. Tables found: {len(tables)}")

# 1. Create the native materialization metadata dictionary
    materialization_metadata = {
        "database_count": len(tables),
        "databases": tables
    }

# 2. Invoke the new function to export the unified JSON
    export_bronze_metadata(
            context=context, 
            materialization_metadata=materialization_metadata, 
            entity_name=DATABASE_TARGET, 
            is_database=True
        )
    
# 3. Yield the result to the Dagster UI
    yield dg.MaterializeResult(
        metadata={
            "database_count": materialization_metadata["database_count"],
            "databases": dg.MetadataValue.json(materialization_metadata["databases"])
        }
    )

# 2. THE "CHILD" ASSET (Table)
@dg.asset(
    name="table_raw_asset", # <--- FILL IN
    group_name="BRONZE",
    description="TEMPLATE: Table [TABLE_NAME]",
    metadata={ # Metadata Table level
        # --- DESCRIPTIVE ---
        "name": "TABLE_NAME",        
        "provenance": dg.MetadataValue.json({
            "source_system": "SOURCE_SYSTEM"})
    }
)
def table_raw_asset(database_raw_asset: str, impala: ImpalaResource):
    """
    Loads the table from Impala. 
    """
    TABLE_NAME = "TABLE_NAME" # <--- FILL IN
    YAML_PATH = CONTRACTS_DIR / f"{TABLE_NAME}.yaml"

    return materialize_bronze_asset(
            context=context,
            yaml_path=str(YAML_PATH), 
            impala_resource=impala, 
            table_name=TABLE_NAME,
            contracts_dir=CONTRACTS_DIR,
            database_name=DATABASE_TARGET
        )
