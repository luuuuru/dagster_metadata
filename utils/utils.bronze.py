import logging
import re
import json
import yaml
import dagster as dg
import ibis
from pathlib import Path
from datetime import datetime
from collections import defaultdict

# ---------------------------------------------------------
# ---  METADATA CREATION  ---
# ---------------------------------------------------------
def extract_table_relationships(contracts_dir: Path) -> dict:
    """
    Scans all YAML contracts, indexes which columns belong to which tables,
    and returns a map of shared columns (implicit relationships).
    """
    column_to_tables = defaultdict(list)

    for yaml_file in contracts_dir.glob("**/*.yaml"):
        if ".ipynb_checkpoints" in yaml_file.parts or yaml_file.name.startswith("."):
            continue
            
        table_name = yaml_file.stem.replace("-", "_")
        
        with open(yaml_file, "r", encoding="utf-8") as f:
            try:
                contract = yaml.safe_load(f)
                columns = contract.get("schema", {}).get("columns", {})
                if columns:
                    for col in columns.keys():
                        column_to_tables[col.lower()].append(table_name)
            except Exception as e:
                print(f"Error reading {yaml_file.name}: {e}")

    # Filter only columns that act as intersections (appear in > 1 table)
    relationships = {col: tables for col, tables in column_to_tables.items() if len(tables) > 1}
    return relationships

def generate_table_specific_relationships_md(table_name: str, contracts_dir: Path) -> str:
    """
    Generates a Markdown string displaying ONLY the relationships 
    relevant to a specific table.
    """
    rels = extract_table_relationships(contracts_dir)
    
    md_lines = [f"### Shared Keys for `{table_name}`\n"]
    found_any = False
    
    for col in sorted(rels.keys()):
        tables = rels[col]
        # Check if the current table has this column
        if table_name in tables:
            # Filter out the current table from the target list
            other_tables = [t for t in tables if t != table_name]
            
            if other_tables:
                tables_str = ", ".join([f"`{t}`" for t in sorted(other_tables)])
                md_lines.append(f"- **{col}**: Connects to {tables_str}")
                found_any = True
                
    if not found_any:
        return f"*No shared columns found for `{table_name}`.*"
        
    return "\n".join(md_lines)

def build_schema_from_yaml(yaml_path: Path) -> tuple[dg.TableSchema, dict]:
    """
    Reads the YAML contract, extracts the columns, and returns:
    1. The dg.TableSchema object
    2. The complete YAML dictionary (for other metadata)
    """
    with open(yaml_path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    
    columns = []
    cols_dict = data.get("schema", {}).get("columns", {})
    
    for col_name, col_info in cols_dict.items():
        columns.append(
            dg.TableColumn(
                name=col_name,
                type=str(col_info.get("type", "string")),
                description=col_info.get("description", "No description provided")
            )
        )
    
    return dg.TableSchema(columns=columns), data

# --- Materialize bronze assets  ---

def materialize_bronze_asset(
    context: dg.AssetExecutionContext,
    yaml_path: str, 
    impala_resource, 
    table_name: str, 
    contracts_dir: Path, 
    database_name: str = ""
) -> dg.Output:  
    """
    Loads a schema contract from a YAML file, builds the structural Dagster TableSchema,
    resolves the connection to the underlying Impala table via Ibis, and returns 
    a standardized dg.Output.
    """
    # 1. Parse the schema using the existing utility function
    t_schema, _ = build_schema_from_yaml(Path(yaml_path))
    tags_columns = {}
    
    # 2. Build the Dagster TableSchema structurally
    schema = dg.TableSchema(
        columns=[
            dg.TableColumn(
                name=col.name,
                type=col.type,
                description=col.description,
                tags=tags_columns.get(col.name, {}) 
            )
            for col in t_schema.columns 
        ]
    )
    
    # 3. Establish connection and target the table
    conn = impala_resource.get_connection()
    result = conn.table(table_name, database=database_name)
    
    # 4. Prepare the metadata 
    relationships_md = generate_table_specific_relationships_md(table_name, contracts_dir)
    
    # MATERIALIZATION (live)
    mat_metadata = {
        "dagster/column_schema": schema,
        "table_relationships": dg.MetadataValue.md(relationships_md),
        "execution_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

    # 5. Export unified JSON
    export_bronze_metadata(
        context=context,
        materialization_metadata=mat_metadata,
        entity_name=table_name
    )
    
    # 6. Return standard Output 
    return dg.Output(
        value=result,         
        metadata=mat_metadata  
    )

# ---------------------------------------------------------
# ---  METADATA EXTRACTION ---
# ---------------------------------------------------------
def _clean_metadata_value(val):
    if isinstance(val, dict):
        return {k: _clean_metadata_value(v) for k, v in val.items()}
    elif isinstance(val, list):
        return [_clean_metadata_value(v) for v in val]
    elif hasattr(val, "columns"):  
        return [{"name": col.name, "type": str(col.type), "description": col.description} for col in val.columns]
    elif hasattr(val, "deps_by_column"):  
        lineage_dict = {}
        for col_name, deps in val.deps_by_column.items():
            lineage_dict[col_name] = [
                {"asset_key": dep.asset_key.to_user_string(), "column_name": dep.column_name}
                for dep in deps
            ]
        return lineage_dict
    elif hasattr(val, "value"):
        return _clean_metadata_value(val.value)
    elif hasattr(val, "data"):
        return _clean_metadata_value(val.data)
    
    return str(val)

def export_bronze_metadata(
    context: dg.AssetExecutionContext, 
    materialization_metadata: dict, 
    entity_name: str, 
    is_database: bool = False
):
    asset_key = context.asset_key
    definition_metadata = {}
    
    if hasattr(context, "asset_spec") and context.asset_spec.metadata:
        definition_metadata = context.asset_spec.metadata
    elif hasattr(context, "assets_def") and hasattr(context.assets_def, "specs_by_key"):
        spec = context.assets_def.specs_by_key.get(asset_key)
        if spec and spec.metadata:
            definition_metadata = spec.metadata

    clean_def_meta = _clean_metadata_value(definition_metadata)
    clean_mat_meta = _clean_metadata_value(materialization_metadata)

    unified_metadata = {
        "asset_name": asset_key.to_user_string(),
        "timestamp": datetime.now().isoformat(),
        "definition_metadata": clean_def_meta,
        "materialization_metadata": clean_mat_meta
    }

    export_dir = Path("")
    export_dir.mkdir(parents=True, exist_ok=True)
    
    prefix = "database_" if is_database else ""
    output_path = export_dir / f"{prefix}{entity_name}_latest.json"
    
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(unified_metadata, f, indent=4, ensure_ascii=False)
        
    entity_type = "Database" if is_database else "Table"
    context.log.info(f"💾 {entity_type} metadata successfully exported to: {output_path}")
