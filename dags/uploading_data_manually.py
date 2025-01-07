from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy.types import String, BigInteger, Float, DateTime, Integer
from datetime import datetime, timedelta
import os
import pandas as pd


def wrangle_data(**context):
    # Get data path from XCom
    data_path = "/opt/airflow/data/ico_data/ico_data_raw.csv"

    if not data_path:
        raise ValueError("No data path found in XCom")

    # Load the raw data
    ico_data = pd.read_csv(data_path)

    # Create a copy of the data
    df = ico_data.copy()

    # Clean and transform data
    df["project_ticker"] = df["project_ticker"].fillna(pd.NA)
    df["project_total_investors"] = df["project_total_investors"].fillna(0)
    df["project_total_raised"] = df["project_total_raised"].replace("—", pd.NA)
    df["project_pre_valuation"] = df["project_pre_valuation"].replace("—", pd.NA)
    df["project_roi"] = df["project_roi"].replace("—", pd.NA)

    # Convert money units
    def convert_money_units(value):
        if pd.isnull(value):
            return pd.NA

        value = str(value).replace("$", "").replace(",", "").strip()
        number = float(
            "".join([char for char in value if char.isdigit() or char == "."])
        )

        if "K" in value:
            return int(number * 1_000)
        if "M" in value:
            return int(number * 1_000_000)
        if "B" in value:
            return int(number * 1_000_000_000)
        if "T" in value:
            return int(number * 1_000_000_000_000)

        return int(number)

    df["project_total_raised"] = df["project_total_raised"].apply(convert_money_units)
    df["project_pre_valuation"] = df["project_pre_valuation"].apply(convert_money_units)

    # Standardize dates
    def standardize_date(date_str):
        if pd.isnull(date_str):
            return pd.NA

        date_str = str(date_str).strip()

        if "Q" in date_str:
            quarter = int(date_str[1])
            year = int(date_str[-4:])
            return pd.to_datetime(f"{year}-{quarter*3}-01")

        if "in" in date_str and "h" in date_str:
            hours = int(date_str.split("h")[0].split()[-1])
            return pd.Timestamp.now() - pd.Timedelta(hours=hours)

        return pd.to_datetime(date_str)

    df["project_date"] = df["project_date"].apply(standardize_date)

    # Standardize ROI
    def standardize_roi(value):
        if pd.isnull(value):
            return pd.NA

        value = str(value).strip().lower()
        value = value.replace("x", "").strip()

        try:
            return float(value)
        except ValueError:
            return pd.NA

    df["project_roi"] = df["project_roi"].apply(standardize_roi)

    # Convert datatypes
    df["project_roi"] = pd.to_numeric(df["project_roi"], errors="coerce")
    df["project_roi"] = df["project_roi"].fillna(pd.NA)

    df["project_total_investors"] = (
        pd.to_numeric(df["project_total_investors"], errors="coerce")
        .round(0)
        .astype("Int64")
    )

    df["project_total_raised"] = (
        pd.to_numeric(df["project_total_raised"], errors="coerce")
        .round(0)
        .astype("Int64")
    )

    df["project_pre_valuation"] = (
        pd.to_numeric(df["project_pre_valuation"], errors="coerce")
        .round(0)
        .astype("Int64")
    )

    # Keep specified columns as strings
    string_columns = [
        "project_name",
        "project_ticker",
        "project_link",
        "project_round",
        "project_categories",
    ]
    df[string_columns] = df[string_columns].astype("string")

    # Save the cleaned data
    cleaned_data_path = os.path.join(os.path.dirname(data_path), "ico_data_cleaned.csv")
    df.to_csv(cleaned_data_path, index=False)

    # Push the cleaned data path to XCom
    context["task_instance"].xcom_push(
        key="cleaned_ico_data_path", value=cleaned_data_path
    )


def store_in_postgres(**context):
    # Get cleaned data path from XCom
    data_path = context["task_instance"].xcom_pull(
        key="cleaned_ico_data_path", task_ids="wrangle_ico_data"
    )

    if not data_path:
        raise ValueError("No cleaned data path found in XCom")

    # Load cleaned data
    df = pd.read_csv(data_path)

    # Convert project_date to datetime
    df["project_date"] = pd.to_datetime(df["project_date"])

    # Connect to PostgreSQL
    pg_hook = PostgresHook(postgres_conn_id="postgres_localhost")

    # Create table with appropriate data types
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS "ICODATA" (
        project_name VARCHAR(255),
        project_ticker VARCHAR(50),
        project_link TEXT,
        project_round VARCHAR(100),
        project_total_raised BIGINT,
        project_pre_valuation BIGINT,
        project_categories VARCHAR(255),
        project_roi FLOAT,
        project_date TIMESTAMP,
        project_total_investors INTEGER
    );
    """

    pg_hook.run(create_table_sql)

    # Define SQLAlchemy types for each column
    sql_dtype = {
        "project_name": String(255),
        "project_ticker": String(50),
        "project_link": String,
        "project_round": String(100),
        "project_total_raised": BigInteger,
        "project_pre_valuation": BigInteger,
        "project_categories": String(255),
        "project_roi": Float,
        "project_date": DateTime,
        "project_total_investors": Integer,
    }

    # Insert data using SQLAlchemy engine
    engine = pg_hook.get_sqlalchemy_engine()
    df.to_sql("ICODATA", engine, if_exists="replace", index=False, dtype=sql_dtype)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "ico_scraping_dag_entire_manually",
    default_args=default_args,
    description="Scrape entire ICO data, wrangle it, and store in PostgreSQL",
    schedule_interval=timedelta(days=1),  # Disable automatic scheduling
    start_date=datetime(2025, 1, 7),  # Fixed start date
    catchup=False,  # Avoid backfilling
) as dag:

    wrangle_task = PythonOperator(
        task_id="wrangle_ico_data",
        python_callable=wrangle_data,
    )

    store_task = PythonOperator(
        task_id="store_in_postgres",
        python_callable=store_in_postgres,
    )

    # Set task dependencies
    wrangle_task >> store_task
