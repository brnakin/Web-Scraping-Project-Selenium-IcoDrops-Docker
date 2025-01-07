import pandas as pd
import numpy as np


# Load the raw data
ico_data = pd.read_csv("data/ico_data/ico_data_raw.csv")

# It is always good practice not to make manipulations on the original data. Therefore we can copy our data.
df = ico_data.copy()

# For the project_ticker column, fill None values with NA. Since None object is not a good practice to use in pandas. None is a python object but pd.NA is a pandas object.
# For the project_total_investors column, fill None values with 0.
# If project_total_raised, project_pre_valuation, and project_roi has value of '-' replace it with NA.
df["project_ticker"] = df["project_ticker"].fillna(pd.NA)
df["project_total_investors"] = df["project_total_investors"].fillna(0)
df["project_total_raised"] = df["project_total_raised"].replace("—", pd.NA)
df["project_pre_valuation"] = df["project_pre_valuation"].replace("—", pd.NA)
df["project_roi"] = df["project_roi"].replace("—", pd.NA)


# Function to clean and convert the "raised" or "pre_valuation" column
def convert_money_units(value):
    if pd.isnull(value):
        return pd.NA

    # Remove $, commas and "Calculated"
    value = str(value).replace("$", "").replace(",", "").strip()

    # Extract the number and unit
    number = float("".join([char for char in value if char.isdigit() or char == "."]))

    if "K" in value:
        return int(number * 1_000)
    if "M" in value:
        return int(number * 1_000_000)
    if "B" in value:
        return int(number * 1_000_000_000)
    if "T" in value:
        return int(number * 1_000_000_000_000)

    return int(number)


# Apply the function to the "raised" column
df["project_total_raised"] = df["project_total_raised"].apply(convert_money_units)

# Apply the function to the "pre_valuation" column
df["project_pre_valuation"] = df["project_pre_valuation"].apply(convert_money_units)


# This will convert all dates to pandas datetime format (YYYY-MM-DD HH:MM:SS). For quarters, it uses the first day of the last month in that quarter.
def standardize_date(date_str):
    if pd.isnull(date_str):
        return pd.NA

    date_str = str(date_str).strip()

    # Handle quarters
    if "Q" in date_str:
        quarter = int(date_str[1])
        year = int(date_str[-4:])
        return pd.to_datetime(f"{year}-{quarter*3}-01")

    # Handle "in Xh" format
    if "in" in date_str and "h" in date_str:
        hours = int(date_str.split("h")[0].split()[-1])
        return pd.Timestamp.now() - pd.Timedelta(hours=hours)

    # Handle standard date formats
    return pd.to_datetime(date_str)


# Apply the function
df["project_date"] = df["project_date"].apply(standardize_date)


# This is for removing x in ROI
def standardize_roi(value):
    if pd.isnull(value):
        return pd.NA

    # Convert to string and clean
    value = str(value).strip().lower()

    # Remove x and any white spaces
    value = value.replace("x", "").strip()

    try:
        # Convert to float
        return float(value)
    except ValueError:
        return pd.NA


# Apply to dataframe
df["project_roi"] = df["project_roi"].apply(standardize_roi)


# Convert "project_roi" to float, preserving NA
df["project_roi"] = pd.to_numeric(df["project_roi"], errors="coerce")
df["project_roi"] = df["project_roi"].fillna(pd.NA)

# Convert "project_total_investors" to Int64 (nullable integer type)
df["project_total_investors"] = (
    pd.to_numeric(
        df["project_total_investors"], errors="coerce"
    )  # Convert to numeric, coercing errors to NaN
    .round(0)  # Round to nearest integer
    .astype("Int64")  # Convert to nullable Int64
)

# Convert "project_total_raised" to Int64 (nullable integer type)
df["project_total_raised"] = (
    pd.to_numeric(df["project_total_raised"], errors="coerce").round(0).astype("Int64")
)

# Convert "project_pre_valuation" to Int64 (nullable integer type)
df["project_pre_valuation"] = (
    pd.to_numeric(df["project_pre_valuation"], errors="coerce").round(0).astype("Int64")
)


# Keep the specified columns as strings
string_columns = [
    "project_name",
    "project_ticker",
    "project_link",
    "project_round",
    "project_categories",
]
df[string_columns] = df[string_columns].astype("string")