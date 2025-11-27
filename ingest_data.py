import sys
import subprocess
from dagster import op
from dagster import asset
@asset
def ingest_data(context):
    script_path = "scripts/ingest_data_into_s3.py"

    try:
        result = subprocess.run(
            [sys.executable, script_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        
        context.log.info("STDOUT:\n" + result.stdout)
        context.log.error("STDERR:\n" + result.stderr)

        if result.returncode != 0:
            # DO NOT raise yet — log first
            context.log.error(f"Script exited with code {result.returncode}")
            raise Exception("Script failed — see STDERR above.")

    except Exception as e:
        context.log.error(str(e))
        raise
