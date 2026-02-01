import pandas as pd
import matplotlib.pyplot as plt

df = pd.read_csv("complex.csv")

df_2023 = df[df["year"] == 2023].copy()
df_2023 = df_2023.sort_values("tests_executed", ascending=False)

df_2023["cum_share"] = (
    df_2023["tests_executed"].cumsum()
    / df_2023["tests_executed"].sum()
)

plt.figure(figsize=(8, 5))
df_2023["cum_share"].reset_index(drop=True).plot()
plt.xlabel("Schemas (sorted by executions)")
plt.ylabel("Cumulative share of executions")
plt.title("2023 execution concentration (top 1% = 88.3%)")
plt.grid(True)

plt.tight_layout()
plt.savefig("2023_execution_concentration.png")

