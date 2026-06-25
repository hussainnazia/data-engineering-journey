import pandas as pd

df = pd.read_csv("sales.csv")

print("Dataset:")
print(df)

print("\nTotal Sales:")
print(df["amount"].sum())

print("\nSales by Product:")
print(df.groupby("product")["amount"].sum())
