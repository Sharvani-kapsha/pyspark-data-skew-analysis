
import matplotlib.pyplot as plt
import numpy as np

# Partition distribution before salting
before = [3163, 3759, 73992, 4070, 3152, 3964, 3806, 4094]

# Partition distribution after salting
after = [12730, 12152, 12991, 13073, 12363, 21041, 3685, 11965]

partitions = np.arange(8)
width = 0.35

fig, ax = plt.subplots(figsize=(12, 6))

ax.bar(partitions - width / 2, before, width, label="Before Salting")
ax.bar(partitions + width / 2, after, width, label="After Salting")

ax.set_title("Spark Data Skew: Before vs After Salting")
ax.set_xlabel("Partition")
ax.set_ylabel("Number of Records")
ax.set_xticks(partitions)
ax.set_xticklabels([f"Partition {i}" for i in partitions])
ax.legend()
ax.grid(axis="y", alpha=0.3)

plt.tight_layout()

plt.savefig("reports/data_skew_analysis.png", dpi=300)
plt.show()

print("Chart saved successfully!")
