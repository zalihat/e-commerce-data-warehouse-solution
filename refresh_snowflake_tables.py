from dagster import op
import snowflake.connector

@op
def refresh_external_tables(context):
    """
    Refresh all external tables in the RAW schema.
    """
    # Snowflake connection
    conn = snowflake.connector.connect(
        user="zalihat.mohammed@st.futminna.edu.ng",
        password="qZYUim5r4ZHAjHA",
        account="NVHFTQO-MP48722",
        warehouse="ECOMMERCE",
        database="ECOMMERCE_DW",
        schema="RAW"  # your raw schema
    )

    cur = conn.cursor()

    try:
        # Get all external tables in the RAW schema
        cur.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'RAW'
              AND table_type = 'EXTERNAL TABLE'
        """)

        tables = [row[0] for row in cur.fetchall()]

        context.log.info(f"Found {len(tables)} external tables to refresh.")

        for table in tables:
            context.log.info(f"Refreshing {table}...")
            cur.execute(f'ALTER EXTERNAL TABLE RAW.{table} REFRESH;')
            context.log.info(f"{table} refreshed successfully.")

    finally:
        cur.close()
        conn.close()
        context.log.info("Finished refreshing all external tables.")
