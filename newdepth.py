import pandas as pd
import matplotlib.pyplot as plt

df = pd.read_csv("q4_results.csv")

summary = (
    df.groupby(["review", "depth"], as_index=False)
      ["distinct_connected_accounts"]
      .sum()
)

for review, group in summary.groupby("review"):
    group = group.sort_values("depth")

    plt.figure(figsize=(8, 5))
    plt.plot(
        group["depth"],
        group["distinct_connected_accounts"],
        marker="o"
    )
    plt.xticks([4, 6, 8, 10])
    plt.xlabel("Maximum path depth (links)")
    plt.ylabel("Distinct connected accounts")
    plt.title(f"Q4 Connected Accounts — Review {review}")
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(f"q4_depth_{review}.png", dpi=150)
    plt.close()

print("Saved Q4 depth plots.")