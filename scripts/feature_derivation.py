import pandas as pd
import numpy as np

# Load offline training samples
df = pd.read_csv("data/train_samples.csv")

# Derived behavior features
df["ctr_view"] = df["view_cnt"] / df["impression_cnt"].replace(0, np.nan)
df["ctr_apply"] = df["apply_cnt"] / df["view_cnt"].replace(0, np.nan)
df["ctr_hire"] = df["hire_cnt"] / df["apply_cnt"].replace(0, np.nan)

# Select final feature set
features_1d = df[
    [
        "job_id",
        "company_id",
        "impression_cnt",
        "view_cnt",
        "apply_cnt",
        "hire_cnt",
        "ctr_view",
        "ctr_apply",
        "ctr_hire",
        "price_label",
    ]
]

# Save derived features
features_1d.to_csv("data/job_features_1d.csv", index=False)
