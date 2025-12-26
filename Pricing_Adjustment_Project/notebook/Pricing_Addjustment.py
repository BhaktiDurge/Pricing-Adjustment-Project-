import pandas as pd



final_df = pd.read_csv("../data/final_pricing_base_table.csv")

print(final_df.shape)
print(final_df.columns)

FAST_SALES_THRESHOLD = 20        # units sold
LOW_INVENTORY_DAYS = 15          # days
HIGH_RETURNS_THRESHOLD = 10      # units
PRICE_INCREASE_PCT = 0.05        # +5%
PRICE_DECREASE_PCT = 0.07        # -7%
COMPETITOR_MATCH_PCT = 0.02      # ±2%


# Minimum allowed price (margin protection)
def minimum_allowed_price(row):
    return row["total_cost"] * (1 + row["minimum_acceptable_margin_%"] / 100)

# Target price (ideal price)
def target_price(row):
    return row["total_cost"] * (1 + row["target_gross_margin_%"] / 100)


# Pricing Logic Function
def pricing_logic(row):

    min_price = minimum_allowed_price(row)
    ideal_price = target_price(row)
    current_price = row["avg_selling_price"]

    # CASE 1: High returns → Quality issue
    if row["return_quantity__(last_30_days)"] > HIGH_RETURNS_THRESHOLD:
        return (
            "Investigate",
            "High return rate indicates possible quality or expectation issues. Pricing change not recommended.",
            current_price
        )

    # CASE 2: Fast selling + low inventory → Increase price
    if (
        row["total_units_sold"] > FAST_SALES_THRESHOLD
        and row["days_of_supply"] < LOW_INVENTORY_DAYS
    ):
        new_price = current_price * (1 + PRICE_INCREASE_PCT)
        new_price = max(new_price, min_price)

        return (
            "Increase Price",
            "Strong demand with low inventory. Price increased to improve margins and control stock.",
            round(new_price, 2)
        )

    # CASE 3: Poor sales + competitor cheaper → Match competitor
    if (
        row["total_units_sold"] < FAST_SALES_THRESHOLD
        and row["lowest_competitor_price"] < current_price
    ):
        new_price = row["lowest_competitor_price"] * (1 + COMPETITOR_MATCH_PCT)
        new_price = max(new_price, min_price)

        return (
            "Decrease Price",
            "Sales underperforming and competitors priced lower. Adjusting price to remain competitive.",
            round(new_price, 2)
        )

    # CASE 4: Stable product → Maintain price
    return (
        "Maintain Price",
        "Sales and inventory levels are stable. Current pricing is appropriate.",
        round(max(current_price, min_price), 2)
    )

# Calling Function
final_df[
    ["pricing_recommendation", "pricing_reason", "recommended_final_price"]
] = final_df.apply(
    pricing_logic,
    axis=1,
    result_type="expand"
)

# Quick Check
print(
    final_df[["sku", "pricing_recommendation", "recommended_final_price"]]
)
print(final_df["pricing_recommendation"].value_counts())


# Saving Updatede file
final_df.to_csv(
    "../data/final_pricing_with_recommendations.csv",
    index=False
)




