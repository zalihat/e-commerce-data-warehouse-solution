# from dagster import Definitions, define_asset_job, load_assets_from_modules

# # Import your asset modules
# import ingest_data
# import dbt_run

# # Load assets from those modules
# all_assets = load_assets_from_modules([ingest_data, dbt_run])

# # Create a job that runs all assets in dependency order
# etl_job = define_asset_job(
#     name="etl_job",
#     selection="*"
# )

# # Register everything
# defs = Definitions(
#     assets=all_assets,
#     jobs=[etl_job],
# )


from dagster import Definitions, define_asset_job, load_assets_from_modules, job

# Import your asset modules
import ingest_data
import dbt_run

# Import your Snowflake refresh op
from refresh_snowflake_tables import refresh_external_tables

# Load assets from your modules
all_assets = load_assets_from_modules([ingest_data, dbt_run])

# Asset-only job (existing)
etl_job = define_asset_job(
    name="etl_job",
    selection="*"
)

# New job combining ingestion + Snowflake refresh + dbt
@job(name="full_etl_pipeline")
def full_etl_pipeline():
    # Run ingestion asset
    ingest_data.ingest_data()
    # Refresh Snowflake external tables
    refresh_external_tables()
    # Run dbt models asset
    dbt_run.dbt_run()

# Register everything
defs = Definitions(
    assets=all_assets,
    jobs=[etl_job, full_etl_pipeline],
)
