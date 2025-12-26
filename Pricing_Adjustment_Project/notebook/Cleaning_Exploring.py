import sys
from pathlib import Path
import pandas as pd
import numpy as np
import os


ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(ROOT_DIR))


from data.importing import (
    pricing_df,
    returns_df,
    inventory_df,
    sales_df,
    competitor_df,
    ads_df
)


print("import succseful")


# Exploring Data

print("Pricing Table",pricing_df.head())
print("Returns Table",returns_df.head())
print("Inventory Table",inventory_df.head())
print("Sales Table",sales_df.head())
print("Competitor Table",competitor_df.head())
print("Ads Table",ads_df.head())


# Function to remove inconsistancy in data and to clean data

def clean_basic(df, name):
    print(f"\n===== {name} =====")

    # 1. Drop duplicates
    before = df.shape[0]
    df = df.drop_duplicates()
    after = df.shape[0]
    print(f"Dropped {before - after} duplicate rows")

    # 2. Clean column names
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace("\n", " ", regex=False)
        .str.replace("-", "_", regex=False)
        .str.replace(" ", "_", regex=False)
    )

    # 3. Fix price & margin columns
    numeric_cols = [
        col for col in df.columns
        if any(x in col for x in ["price", "cost", "margin"])
    ]

    for col in numeric_cols:
        df[col] = (
            df[col]
            .astype(str)
            .str.replace(r"[₹$,%]", "", regex=True)
        )
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # 4. Basic exploration
    print("\nINFO:")
    df.info()

    print("\nDESCRIBE:")
    print(df.describe(include="all"))

    return df

# Function to handle null values
def handle_nulls(df, name):
    print(f"\n--- Handling Nulls: {name} ---")

    for col in df.columns:
        if df[col].dtype in ["int64", "float64"]:
            df[col] = df[col].fillna(0)
        else:
            df[col] = df[col].fillna("Unknown")

    print("Nulls handled successfully")
    return df

# Funtion for Sales Aggregation Per SKU
def aggregate_sales_per_sku(sales_df):
    sales_agg = (
        sales_df
        .groupby("sku", as_index=False)
        .agg(
            total_units_sold=("units_ordered", "sum"),
            total_sales=("ordered_product_sales", "sum"),
            avg_selling_price=("ordered_product_sales", "mean")
        )
    )
    return sales_agg


# Funtion for ads aggregation per SKU
def aggregate_ads_per_sku(ads_df):
    ads_agg = (
        ads_df
        .groupby("sku", as_index=False)
        .agg(
            total_ad_spend=("spend", "sum"),
            total_clicks=("clicks", "sum"),
            total_impressions=("impressions", "sum"),
            avg_cpc=("costperclick", "mean"),
            sales_30d=("sales30d", "sum")
        )
    )
    return ads_agg





# Calling the Function for each df
pricing_df = clean_basic(pricing_df, "Pricing Data")
competitor_df = clean_basic(competitor_df, "Competitor Data")
sales_df = clean_basic(sales_df, "Historical Sales")
inventory_df = clean_basic(inventory_df, "Inventory Health")
returns_df = clean_basic(returns_df, "Returns Data")
ads_df = clean_basic(ads_df, "Ads Performance")


# Calling the handle null fuction
sales_df = handle_nulls(sales_df, "Sales")
ads_df = handle_nulls(ads_df, "Ads")


# Calling sales_agg funtion
sales_agg = aggregate_sales_per_sku(sales_df)
print("\nSales Aggregated Per SKU:")
print(sales_agg.head())


# Calling ads_agg funtion
ads_agg = aggregate_ads_per_sku(ads_df)
print("\nAds Aggregated Per SKU:")
print(ads_agg.head())




# Making single Aggergate Table for Pricing Adjustment
pricing_cols = [
    "sku",
    "product_role",
    "cost",
    "fba_fee",
    "storage_fee",
    "handling_cost",
    "minimum_acceptable_margin_%",
    "target_gross_margin_%"
]

pricing_sel = pricing_df[pricing_cols].copy()

# Convert fee columns to numeric
for col in ["fba_fee", "storage_fee", "handling_cost"]:
    pricing_sel[col] = (
        pricing_sel[col]
        .astype(str)
        .str.replace("%", "", regex=False)
        .str.replace("$", "", regex=False)
        .astype(float)
    )


# total cost per unit
pricing_sel["total_cost"] = (
    pricing_sel["cost"]
    + pricing_sel["fba_fee"]
    + pricing_sel["storage_fee"]
    + pricing_sel["handling_cost"]
)

competitor_cols = [
    "sku",
    "avg_competitor_price",
    "lowest_competitor_price",
    "highest_competitor_price",
    "competitor_count"
]

competitor_sel = competitor_df[competitor_cols].copy()


inventory_cols = [
    "sku",
    "available",
    "total_inventory",
    "days_of_supply",
    "weeks_of_cover_t30",
    "weeks_of_cover_t90"
]

inventory_sel = inventory_df[inventory_cols].copy()

returns_cols = [
    "sku",
    "return_quantity__(last_30_days)",
    "return_quantity__(last_60_days)",
    "return_quantity__(last_90_days)"
]

returns_sel = returns_df[returns_cols].copy()


final_df = pricing_sel \
    .merge(competitor_sel, on="sku", how="left") \
    .merge(sales_agg, on="sku", how="left") \
    .merge(inventory_sel, on="sku", how="left") \
    .merge(returns_sel, on="sku", how="left") \
    .merge(ads_agg, on="sku", how="left")


print(final_df.head())
print(final_df.info())


# Saving file as csv
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")

os.makedirs(DATA_DIR, exist_ok=True)

file_path = os.path.join(DATA_DIR, "final_pricing_base_table.csv")

final_df = final_df.loc[:, ~final_df.columns.duplicated()]
final_df.to_csv(file_path, index=False)

print("CSV SAVED AT:", file_path)








