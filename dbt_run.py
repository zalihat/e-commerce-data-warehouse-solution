import sys
import subprocess
from dagster import asset

@asset(deps=["ingest_data"])   # <-- This creates the dependency
def dbt_run(context):
    dbt_project_path = "dbt_project"

    context.log.info("Running dbt run...")

    result = subprocess.run(
        ["dbt", "run"],                   # or: [sys.executable, "-m", "dbt", "run"]
        cwd=dbt_project_path,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )

    context.log.info("DBT STDOUT:\n" + result.stdout)
    context.log.error("DBT STDERR:\n" + result.stderr)

    if result.returncode != 0:
        raise Exception("dbt run failed (see stderr).")

    context.log.info("dbt run finished!")
