from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import (
    TimeoutException,
    NoSuchElementException,
    ElementNotInteractableException,
    ElementClickInterceptedException,
)
from sqlalchemy.types import String, BigInteger, Float, DateTime, Integer
from datetime import datetime, timedelta
import time
import os
import pandas as pd


def wait_and_click(driver, xpath, timeout=10):
    print(f"Attempting to click element with xpath: {xpath}")
    element = WebDriverWait(driver, timeout).until(
        EC.element_to_be_clickable((By.XPATH, xpath))
    )
    print(f"Element found, clicking on it: {xpath}")
    element.click()


def load_all_content(driver):
    print("Loading all content (showing more items)...")
    while True:
        try:
            wait_and_click(driver, "//button[contains(text(), 'Show more')]", timeout=5)
            time.sleep(3)
        except TimeoutException:
            print("No more 'Show more' button found, in given time.")
            break
        except ElementNotInteractableException:
            print("No more 'Show more' button found, all content loaded.")
            break
        except ElementClickInterceptedException:
            print("Element is not clickable at the moment, waiting...")
            time.sleep(5)


def extract_ico_data(driver, index):
    data = {}
    try:
        base_xpath = f'//*[@id="table-list"]/li[{index}]'
        xpaths = {
            "project_name": f"{base_xpath}/div[2]/div/a/p[1]",
            "project_ticker": f"{base_xpath}/div[2]/div/a/p[2]",
            "project_link": f"{base_xpath}/div[2]/div/a",
            "project_round": f"{base_xpath}/div[3]/p",
            "project_total_raised": f"{base_xpath}/div[4]/p",
            "project_pre_valuation": f"{base_xpath}/div[5]/p[1]",
            "project_categories": f"{base_xpath}/div[8]/p",
            "project_roi": f"{base_xpath}/div[9]/p[1]",
            "project_date": f"{base_xpath}/div[10]/time",
        }

        for key, xpath in xpaths.items():
            try:
                if key == "project_link":
                    data[key] = driver.find_element(By.XPATH, xpath).get_attribute(
                        "href"
                    )
                else:
                    data[key] = driver.find_element(By.XPATH, xpath).text
                print(f"Extracted {key}: {data[key]}")
            except NoSuchElementException:
                data[key] = None
                print(f"{key} not found.")

        investors_count = 0
        try:
            for i in range(1, 4):
                try:
                    driver.find_element(By.XPATH, f"{base_xpath}/div[6]/ul/li[{i}]/img")
                    investors_count += 1
                except NoSuchElementException:
                    break

            try:
                additional = driver.find_element(
                    By.XPATH, f"{base_xpath}/div[6]/ul/li[4]/span"
                ).text
                investors_count += int(additional.replace("+", ""))
            except NoSuchElementException:
                pass
        except Exception:
            pass

        data["project_total_investors"] = (
            investors_count if investors_count > 0 else None
        )

        print(f"Extracted project_total_investors: {investors_count}")

    except Exception as e:
        print(f"Error extracting data: {e}")

    return data


def scrape_data(**context):
    print("Starting the web scraping process...")

    # Define paths for Chrome and ChromeDriver
    chrome_binary_path = "/opt/chrome/chrome"
    chromedriver_path = "/opt/chromedriver/chromedriver"

    # Check versions
    try:
        chrome_version = os.popen(f"{chrome_binary_path} --version").read().strip()
        chromedriver_version = os.popen(f"{chromedriver_path} --version").read().strip()
        print(f"Chrome version: {chrome_version}")
        print(f"ChromeDriver version: {chromedriver_version}")
    except Exception as e:
        print(f"Error checking versions: {e}")

    # Configure Chrome Options
    chrome_options = Options()
    chrome_options.binary_location = chrome_binary_path

    # Essential arguments
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")

    # Additional stability arguments
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--remote-debugging-port=9222")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("--disable-infobars")
    chrome_options.add_argument("--single-process")
    chrome_options.add_argument("--disable-setuid-sandbox")
    chrome_options.add_argument("--disable-software-rasterizer")

    print(f"Chrome binary location set to: {chrome_binary_path}")

    # Set up ChromeDriver service with logging
    service = Service(
        executable_path=chromedriver_path, log_path="/opt/airflow/logs/chromedriver.log"
    )
    print(f"ChromeDriver path set to: {chromedriver_path}")

    # Initialize WebDriver
    driver = webdriver.Chrome(service=service, options=chrome_options)
    driver.set_window_size(1920, 1080)
    driver.implicitly_wait(10)
    print("WebDriver initialized successfully.")

    all_data = []
    try:
        print("Navigating to the ICO Drops page...")
        driver.get("https://icodrops.com/category/ended-ico/")

        try:
            wait_and_click(driver, "//button[contains(text(), 'Agree')]")
        except TimeoutException:
            print("Consent button not found or already agreed.")

        time.sleep(3)
        load_all_content(driver)

        ico_count = len(driver.find_elements(By.XPATH, '//*[@id="table-list"]/li'))
        print(f"Found {ico_count} ICOs on the page.")

        for i in range(1, ico_count + 1):
            print(f"\nProcessing ICO {i}/{ico_count}")
            data = extract_ico_data(driver, i)
            all_data.append(data)

    finally:
        driver.quit()

    # In the scrape_data function, just before pushing to XCom
    print(f"Scraped {len(all_data)} ICOs.")
    if all_data:
        df = pd.DataFrame(all_data)
        # Define the directory where you want to save the CSV file
        directory = "/opt/airflow/data/ico_data"
        # Make sure the directory exists, create it if it doesn't
        os.makedirs(directory, exist_ok=True)
        # Define the CSV file path
        csv_file_path = os.path.join(directory, "ico_data_raw.csv")
        # Save DataFrame to CSV
        df.to_csv(csv_file_path, index=False)
        print(f"CSV file saved at {csv_file_path}")

        context["task_instance"].xcom_push(key="ico_data_path", value=csv_file_path)
    else:
        pass


def wrangle_data(**context):
    # Get data path from XCom
    data_path = context["task_instance"].xcom_pull(
        key="ico_data_path", task_ids="scrape_ico_data"
    )

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
    "ico_scraping_dag_all",
    default_args=default_args,
    description="Scrape entire ICO data, wrangle it, and store in PostgreSQL",
    schedule_interval=timedelta(days=1),  # Disable automatic scheduling
    start_date=datetime(2025, 1, 7),  # Fixed start date
    catchup=False,  # Avoid backfilling
) as dag:

    scrape_task = PythonOperator(
        task_id="scrape_ico_data",
        python_callable=scrape_data,
    )

    wrangle_task = PythonOperator(
        task_id="wrangle_ico_data",
        python_callable=wrangle_data,
    )

    store_task = PythonOperator(
        task_id="store_in_postgres",
        python_callable=store_in_postgres,
    )

    # Set task dependencies
    scrape_task >> wrangle_task >> store_task
