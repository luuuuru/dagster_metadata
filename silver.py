import dagster as dg
import ibis
from pathlib import Path

from resources import ImpalaResource 
# Import the parent bronze assets to maintain the lineage chain
from bronze import bronze_db_asset, table_a_raw, table_b_raw

# ---------------------------------------------------------
# --- SILVER LAYER (Metadata Template) ---
# ---------------------------------------------------------

@dg.asset(
    group_name="SILVER",
    ins={
        "input_a": dg.AssetIn("table_a_raw"),
        "input_b": dg.AssetIn("table_b_raw")
    },
    description="TEMPLATE: Silver layer transformation "
)
def silver_transformed_asset(input_a: ibis.Table, input_b: ibis.Table):
    """
    Silver Layer Template: Performs transformations while dynamically 
    tracking column lineage and structural metadata.
    """
    # 1. TRANSFORMATION LOGIC
    # ---------------------------------------------------------
    # Example: result = input_a.left_join(input_b, "key_column")
    result = input_a # <--- REPLACE WITH YOUR JOIN/TRANSFORMATION
    
    # 2. METADATA EXTRACTION (The "What")
    # ---------------------------------------------------------
    operation = result.op() 
    op_name = type(operation).__name__ 
  
    # Logic to detect transformation method (e.g., Join type)
    method = None
    if op_name == "JoinChain":
        if operation.rest:
            method = operation.rest[0].how 
    elif hasattr(operation, "how"):
        method = operation.how
        
    # 3. DYNAMIC COLUMN LINEAGE & SCHEMA GENERATION
    # ---------------------------------------------------------
    deps_by_column = {}
    table_columns = []

    for col in result.columns:
        # Create schema entry for UI
        table_columns.append(dg.TableColumn(name=col))
        
        # Lineage tracing logic
        deps = []
        # Example Logic:
        # if col in input_a.columns:
        #     deps.append(dg.TableColumnDep(asset_key=dg.AssetKey("table_a_raw"), column_name=col))
        
        if deps:
            deps_by_column[col] = deps
                
    # 4. MATERIALIZATION (Metadata & Metrics)
    # ---------------------------------------------------------
    return dg.MaterializeResult(
        value=result, 
        metadata={
            # --- STRUCTURAL ---
            "dagster/column_schema": dg.TableSchema(columns=table_columns),
            
            # --- PROVENANCE ---
            "dagster/column_lineage": dg.TableColumnLineage(deps_by_column=deps_by_column),
            "transformation_logic": dg.MetadataValue.json({
                "type": op_name, 
                "method": method.capitalize() if method else "Transformation",
            }),
            
            # --- EXPRESSION ---
            "expression": dg.MetadataValue.md(f"```python\n{result}\n```"),
            
            # --- DYNAMIC METRICS ---
            "column_count": len(result.columns),
            "columns_list": dg.MetadataValue.json(list(result.columns))
        }
    )

# ---------------------------------------------------------
# --- SILVER QUALITY CHECKS TEMPLATE ---
# ---------------------------------------------------------

@dg.asset_check(
    asset=silver_transformed_asset,
    description="TEMPLATE: General integrity check"
)
def silver_integrity_check(silver_transformed_asset: ibis.Table, impala: ImpalaResource):
    """
    Template for data validation in the Silver layer.
    """
    conn = impala.get_connection()
    
    # 1. VALIDATION LOGIC (Ibis Expression)
    check_expr = silver_transformed_asset.aggregate(
        error_count=silver_transformed_asset["some_column"].null_count()
    )
    
    # 2. EXECUTION
    res = conn.execute(check_expr)
    val = int(res.iloc[0]['error_count']) if hasattr(res, 'iloc') else int(res['error_count'])
    
    passed = (val == 0)
    
    # 3. RESULTS
    return dg.AssetCheckResult(
        passed=passed,
        metadata={
            # --- RULE ---
            "rule": "Description of the validation rule",
            # --- EXPRESSION ---
            "expression": dg.MetadataValue.md(f"```python\n{check_expr}\n```"),
           # --- DYNAMIC METRICS ---
            "error_metric": val,
        }
    )
