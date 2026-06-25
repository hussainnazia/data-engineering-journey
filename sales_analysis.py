import pandas as pd

# Read data
df = pd.read_csv("sales.csv")

# Basic information
print("=== Dataset ===")
print(df)

print("\n=== Total Sales ===")
print(df["amount"].sum())

print("\n=== Average Sale Amount ===")
print(df["amount"].mean())

print("\n=== Sales by Product ===")
print(df.groupby("product")["amount"].sum())

print("\n=== Top Product ===")
print(
    df.groupby("product")["amount"]
      .sum()
      .sort_values(ascending=False)
      .head(1)
)
