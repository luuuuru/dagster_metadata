from dagster import Definitions, load_assets_from_package_module, load_asset_checks_from_package_module

# Import the asset package and the custom Resource class
from .defs import assets as assets_package
from .defs.resources import ImpalaResource

# 1. ASSET DISCOVERY
# Dagster automatically scans the 'assets' package to find all @asset definitions
all_assets = load_assets_from_package_module(assets_package)

# 2. CHECK DISCOVERY
# Dagster automatically scans the same package to find all @asset_check definitions
all_checks = load_asset_checks_from_package_module(assets_package)

# 3. GLOBAL DEFINITIONS
# Linking assets, checks, and resources into the project scope
defs = Definitions(
    assets=all_assets,
    asset_checks=all_checks,
    resources={
        # RESOURCE BINDING:
        # When an asset or check requests a parameter named 'impala', 
        # Dagster will inject this ImpalaResource instance.
        "impala": ImpalaResource()
    }
)
