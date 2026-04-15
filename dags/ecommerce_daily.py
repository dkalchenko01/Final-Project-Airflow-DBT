import io

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag, task
from pendulum import datetime
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import pandas as pd
import duckdb


@dag(
    schedule="@daily",
    start_date=datetime(2026, 4, 14),
    catchup=False,
    description="Fetching data daily from MySql",
    max_consecutive_failed_dag_runs=3
)
def daily_data_parsing():

    @task
    def extract_load_products_mysql_duckdb():
        with duckdb.connect("/usr/local/airflow/ecommerce_dbt/final_project_ecommerce.duckdb") as duckdb_conn:
            duckdb_conn.execute("""
                            CREATE TABLE IF NOT EXISTS raw_products (
                                id INT PRIMARY KEY,
                                id_warehouse INT,
                                warehouse_city VARCHAR(50),
                                warehouse_address VARCHAR(255),
                                responsible_warehouse INT,
                                name VARCHAR(100),
                                color VARCHAR(50),
                                category VARCHAR(50),
                                description TEXT,
                                price DECIMAL(10,2),
                                left_in_stock INT,
                                updated_at DATE,
                                processed_at TIMESTAMP );
                         """)
            prod_last_id = duckdb_conn.execute("SELECT MAX(id) FROM raw_products").fetchone()[0] or 0
            print(f"RAW_PRODUCTS: Loading starts from id-{prod_last_id}")


            mysql_hook = MySqlHook(mysql_conn_id='ecommerce')
            raw_products_df = mysql_hook.get_pandas_df(
                f"SELECT * FROM ecommerce.raw_products WHERE id > %s;",
                parameters=(prod_last_id,)
            )

            if raw_products_df.empty:
                print("No new data found in RAW_PRODUCTS.")
                return

            raw_products_df['processed_at'] = pd.Timestamp.now()

            duckdb_conn.register("raw_products_df", raw_products_df)
            duckdb_conn.execute("INSERT OR IGNORE INTO raw_products SELECT * FROM raw_products_df")
            print(f"Success! Loaded {len(raw_products_df)} rows into RAW_PRODUCTS.")

    @task
    def extract_load_people_mysql_duckdb():
        with duckdb.connect("/usr/local/airflow/ecommerce_dbt/final_project_ecommerce.duckdb") as duckdb_conn:
            duckdb_conn.execute("""
                                        CREATE TABLE IF NOT EXISTS raw_people (
                                            id_person INT PRIMARY KEY,
                                            role VARCHAR(50),
                                            name VARCHAR(100),
                                            phone_num VARCHAR(30),
                                            manager_id INT,
                                            head INT,
                                            department VARCHAR(50),
                                            start_date DATE,
                                            processed_at TIMESTAMP
                                        );
                                    """)
            peop_last_id = duckdb_conn.execute("SELECT MAX(id_person) FROM raw_people").fetchone()[0] or 0
            print(f"RAW_PEOPLE: Loading starts from id-{peop_last_id}")

            mysql_hook = MySqlHook(mysql_conn_id='ecommerce')
            raw_people_df = mysql_hook.get_pandas_df(
                f"SELECT * FROM ecommerce.raw_people WHERE id_person > %s;",
                parameters=(peop_last_id,)
            )

            if raw_people_df.empty:
                print("No new data found in RAW_PEOPLE.")
                return

            raw_people_df['processed_at'] = pd.Timestamp.now()

            duckdb_conn.register("raw_people_df", raw_people_df)
            duckdb_conn.execute("INSERT OR IGNORE INTO raw_people SELECT * FROM raw_people_df")
            print(f"Success! Loaded {len(raw_people_df)} rows into RAW_PEOPLE.")

    @task
    def extract_load_geo_minio_duckdb():
        s3_hook = S3Hook(aws_conn_id='minio_conn')
        file = s3_hook.read_key(key='raw_geo_directory.csv', bucket_name='my-dbt-source')
        df = pd.read_csv(io.StringIO(file))
        with duckdb.connect("/usr/local/airflow/ecommerce_dbt/final_project_ecommerce.duckdb") as duckdb_conn:
            df['processed_at'] = pd.Timestamp.now()
            duckdb_conn.register("raw_geo_directory", df)
            duckdb_conn.execute("create or replace table raw_geo_directory as select * from raw_geo_directory")
            print(f"Success! Loaded {len(df)} rows from MinIO into raw_geo_directory.")


    run_dbt_daily = BashOperator(
        task_id='run_dbt_daily',
        bash_command="""
                sleep 5 && \
                cd /usr/local/airflow/ecommerce_dbt && \
                dbt build --select tag:daily --profiles-dir .
            """
    )


    extract_load_products_mysql_duckdb() >> extract_load_people_mysql_duckdb() >> extract_load_geo_minio_duckdb() >> run_dbt_daily
daily_data_parsing()