from airflow.sdk import dag, task
from pendulum import datetime
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.standard.operators.bash import BashOperator
import pandas as pd
import duckdb

@dag(
    schedule="@hourly",
    start_date=datetime(2026, 4, 14),
    catchup=False,
    description="Fetching data hourly from MySql and MinIO",
    max_consecutive_failed_dag_runs=3
)
def hourly_data_parsing():

    @task
    def extract_load_mysql_duckdb():
        with duckdb.connect("/usr/local/airflow/ecommerce_dbt/final_project_ecommerce.duckdb") as duckdb_conn:
            duckdb_conn.execute("""
                            CREATE TABLE IF NOT EXISTS raw_order_items (
                                id_order_item INT PRIMARY KEY,
                                id_order INT,
                                id_product INT,
                                id_customer INT,
                                id_manager INT,
                                quantity INT,
                                price DECIMAL(10,2),
                                purchase_date DATE,
                                payment_method VARCHAR(50),
                                shipping_method VARCHAR(50),
                                discount INT,
                                processed_at TIMESTAMP
                            );
                        """)
            last_id = duckdb_conn.execute("SELECT MAX(id_order_item) FROM raw_order_items").fetchone()[0] or 0
            print(f"Loading starts from id-{last_id}")


            mysql_hook = MySqlHook(mysql_conn_id='ecommerce')
            raw_items_df = mysql_hook.get_pandas_df(
                f"SELECT * FROM ecommerce.raw_order_items WHERE id_order_item > %s;",
                parameters=(last_id,)
            )

            if raw_items_df.empty:
                print("No new data found in RAW_ORDER_ITEMS.")
                return

            raw_items_df['processed_at'] = pd.Timestamp.now()

            duckdb_conn.register("raw_items_df", raw_items_df)
            duckdb_conn.execute("INSERT OR IGNORE INTO raw_order_items SELECT * FROM raw_items_df")
            print(f"Success! Loaded {len(raw_items_df)} rows into RAW_ORDER_ITEMS.")

    @task
    def extract_load_status_logs():
        json_path = "/usr/local/airflow/data/ecommerce/status_logs.json"
        df = pd.read_json(json_path)
        df["processed_at"] = pd.Timestamp.now()

        with duckdb.connect("/usr/local/airflow/ecommerce_dbt/final_project_ecommerce.duckdb") as duckdb_conn:
            duckdb_conn.register("raw_status_logs_df", df)
            duckdb_conn.execute("create or replace table raw_status_logs as select * from raw_status_logs_df")
            print(f"Success! Loaded {len(df)} rows from JSON into raw_status_logs.")

    run_dbt_hourly = BashOperator(
        task_id='run_dbt_hourly',
        bash_command="""
                sleep 5 && \
                cd /usr/local/airflow/ecommerce_dbt && \
                dbt build --select tag:hourly --profiles-dir .
            """
    )

    extract_load_mysql_duckdb() >> extract_load_status_logs() >> run_dbt_hourly
hourly_data_parsing()