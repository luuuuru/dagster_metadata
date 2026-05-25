import dagster as dg
import ibis
from pathlib import Path

from ..resources import ImpalaResource 
# Import the parent bronze assets to maintain the lineage chain
from .bronze import bronze_db_asset, table_a_raw, table_b_raw, bronze_asset_name # <-- FILL IN

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
# --- SILVER QUALITY ASSET CHECKS TEMPLATE ---
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
            "error_metric": val
        }

    )

# ---------------------------------------------------------
# --- SILVER LAYER (Inline Checks & Dynamic Metadata Template) ---
# ---------------------------------------------------------
@dg.asset(
    name="silver_asset_name", # <-- FILL IN
    group_name="SILVER",
    deps=[bronze_asset_name], # <-- FILL IN
    description="TEMPLATE: Cleaning, transformations, and intermediate validations (Inline Checks)",
    # INTERMEDIATE CHECKS DECLARATION
    check_specs=[
        dg.AssetCheckSpec(name="format_preserves_row_count", asset="silver_asset_name"),
        dg.AssetCheckSpec(name="filter_reduces_row_count", asset="silver_asset_name"),
        dg.AssetCheckSpec(name="join_validation", asset="silver_asset_name"),
    ]
)
def template_silver_asset(context: dg.AssetExecutionContext, impala: ImpalaResource):
    """
    Silver Template: Executes sequential transformations, emitting 
    intermediate AssetCheckResults to validate row loss or retention.
    """
    client = impala.get_connection()
    
    # 0. READING RAW/BRONZE TABLES
    # ---------------------------------------------------------
    base_table = client.table('raw_table_name', database='raw_schema') # <-- FILL IN
    initial_count = int(base_table.count().execute())

    # 1. PHASE 1: TRANSFORMATIONS AND CASTING
    # ---------------------------------------------------------
    base_table = base_table.mutate(
        column_1=base_table.column_1.cast('int64'), # <-- FILL IN LOGIC
    )
    
    phase_1_count = int(base_table.count().execute())
    
    yield dg.AssetCheckResult(
        passed=(phase_1_count == initial_count),
        check_name="format_preserves_row_count",
        metadata={
            "rule": "Casting primitive types must not alter the volume of records.",
            "pre_count": initial_count,
            "post_count": phase_1_count,
            "difference": initial_count - phase_1_count
        }
    )

    # 2. PHASE 2: FILTERING
    # ---------------------------------------------------------
    base_table = base_table.filter(
        base_table.date_column >= '2014-01-01' # <-- FILL IN LOGIC
    )
    
    phase_2_count = int(base_table.count().execute())
    
    yield dg.AssetCheckResult(
        passed=(phase_2_count <= phase_1_count),
        check_name="filter_reduces_row_count",
        metadata={
            "rule": "Filters must reduce or maintain the row volume.",
            "pre_count": phase_1_count,
            "post_count": phase_2_count,
            "excluded_rows": phase_1_count - phase_2_count
        }
    )

    # 3. PHASE 3: JOINS WITH DYNAMIC METADATA EXTRACTION
    # ---------------------------------------------------------
    join_table = client.table('other_table', database='schema').select('id') # <-- FILL IN LOGIC
    
    base_table = base_table.semi_join(
        join_table,
        base_table.id == join_table.id
    )
    
    phase_3_count = int(base_table.count().execute())
    
    # Automatic metadata extraction from the Ibis operation
    op_type = type(base_table.op()).__name__ 
    query_logic = ibis.to_sql(base_table)
    
    yield dg.AssetCheckResult(
        passed=(phase_3_count <= phase_2_count and phase_3_count != 0),
        check_name="join_validation",
        metadata={
            # --- TRANSFORMATION METADATA ---
            "transformation_logic": dg.MetadataValue.json({"type": op_type}),
            "expression": dg.MetadataValue.md(f"```sql\n{query_logic}\n```"),
            
            # --- CHECK METADATA ---
            "rule": "The join must not result in an empty table.",
            "pre_count": phase_2_count,
            "post_count": phase_3_count
        }
    )

    # 4. DYNAMIC COLUMN LINEAGE EXTRACTION (PROVENANCE)
    # ---------------------------------------------------------
    deps_by_column = {}
    table_columns = []

    for col in base_table.columns:
        table_columns.append(dg.TableColumn(name=col))
        # Assumes all resulting columns come from the main bronze asset
        deps_by_column[col] = [
            dg.TableColumnDep(asset_key=dg.AssetKey("bronze_asset_name"), column_name=col) # <-- FILL IN
        ]
    
    # 5. MATERIALIZATION (METADATA & METRICS)
    # ---------------------------------------------------------
    yield dg.Output(
        value=base_table,
        metadata={
            # --- METRICS ---
            "status": "Processing and checks completed",
            "initial_raw_rows": initial_count,
            "final_silver_rows": phase_3_count,
            "cumulative_retention_percentage": f"{(phase_3_count / initial_count * 100):.2f}%" if initial_count > 0 else "0%",
            
            # --- STRUCTURAL ---
            "dagster/column_schema": dg.TableSchema(columns=table_columns),
            
            # --- PROVENANCE ---
            "dagster/column_lineage": dg.TableColumnLineage(deps_by_column=deps_by_column),
            
            # --- DYNAMIC METRICS ---
            "column_count": len(base_table.columns),
            "columns_list": dg.MetadataValue.json(list(base_table.columns))
        }
    )
