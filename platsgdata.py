
import pandas as pd
import matplotlib.pyplot as plt

df = pd.read_csv("q2_daily.csv", parse_dates=["transfer_day"])
df = df.sort_values("transfer_day")

# Chart 1: Daily transfer count
plt.figure(figsize=(12, 5))
plt.plot(
    df["transfer_day"],
    df["transfer_count"],
    marker="o"
)
plt.title("Daily Mobile Money Transfer Count — September 2026")
plt.xlabel("Date (CAT)")
plt.ylabel("Number of transfers")
plt.xticks(rotation=45)
plt.grid(True, alpha=0.3)
plt.tight_layout()
plt.savefig("q2_daily_transfer_count.png", dpi=200)
plt.show()

# Chart 2: Daily transfer value
plt.figure(figsize=(12, 5))
plt.plot(
    df["transfer_day"],
    df["total_amount_rwf"],
    marker="o"
)
plt.title("Daily Mobile Money Transfer Value — September 2026")
plt.xlabel("Date (CAT)")
plt.ylabel("Total transfer value (RWF)")
plt.xticks(rotation=45)
plt.grid(True, alpha=0.3)
plt.tight_layout()
plt.savefig("q2_daily_transfer_value.png", dpi=200)
plt.show()

# Reconciliation summary
print("Days returned:", len(df))
print("Daily transfer count sum:", df["transfer_count"].sum())
print("Daily amount sum (RWF):", df["total_amount_rwf"].sum())
