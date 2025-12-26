import pandas as pd
from pathlib import Path

BASE_DIR = Path(__file__).parent

pricing_df = pd.read_csv(BASE_DIR / "Pricing_Data.csv")
competitor_df = pd.read_csv(BASE_DIR / "Competitor_Data.csv")
sales_df = pd.read_csv(BASE_DIR / "Historical_Sales.csv")
inventory_df = pd.read_csv(BASE_DIR / "Inventory_Health.csv")
returns_df = pd.read_csv(BASE_DIR / "Returns_Data.csv")
ads_df = pd.read_csv(BASE_DIR / "Ads_Performance.csv")


