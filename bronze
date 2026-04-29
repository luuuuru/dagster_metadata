import dagster as dg
import ibis
from pathlib import Path

from resources import ImpalaResource 
# Import your empty or filled schemas here
from column_schemas import template_column_schema

# ---------------------------------------------------------
# --- BRONZE LAYER (Metadata Template) ---
# ---------------------------------------------------------

# 1. THE "PARENT" ASSET (Database / Higher Hierarchical Level)
@dg.asset(
    group_name="BRONZE",
    description="TEMPLATE: Connection and validation of database [DB_NAME]",
    metadata={
        "type": "Administrative",
        "db_engine": "Impala/Cloudera"    
}
)
def database_raw_asset(impala: ImpalaResource) -> str:
    """Validates the existence of the database and returns its name."""
    conn = impala.get_connection()
    db_name = "YOUR_DB_NAME" # <--- FILL IN
    tables = conn.list_tables(database=db_name)
    dg.get_dagster_logger().info(f"Database: {db_name}. Tables found: {len(tables)}")

    return db_name

# 2. THE "CHILD" ASSET (Table / Static Definition)
@dg.asset(
    group_name="BRONZE",
    description="TEMPLATE: Table [TABLE_NAME]",
    metadata={
        # --- DESCRIPTIVE ---
        "name": "TECHNICAL_TABLE_NAME",
        "display_name": "HUMAN_READABLE_NAME",
        "clinical_coverage": "N/A", # e.g., Patients with pathology X
        "temporal_coverage": "N/A", # e.g., 2020-2024
        "contextual_coverage": "N/A", # e.g., Source System X
        
        # --- STRUCTURAL ---
        "dagster/column_schema": None, # <--- ASSIGN YOUR TableSchema OBJECT
        "linkage": dg.MetadataValue.md("- **Relationship**: N/A"), 
        
        # --- PROVENANCE ---
        "provenance": dg.MetadataValue.json({
            "source_system": "SOURCE_SYSTEM",
            "update_frequency": "DAILY/MONTHLY",
            "rules_applied": ["Direct Raw Load"]
        }),

        # --- SEMANTIC ---
        "semantics": dg.MetadataValue.json({
            "standard_mapping": "N/A", # e.g., OMOP, SNOMED, LOINC
        })
    }
)
def table_raw_asset(database_raw_asset: str, impala: ImpalaResource):
    """
    Loads the table from Impala. 
    Note: The metadata defined here represents the DEFINITION (Input).
    """
    conn = impala.get_connection()
    table_name = "IMPALA_TABLE_NAME" # <--- FILL IN
    return conn.table(table_name, database=database_raw_asset)
