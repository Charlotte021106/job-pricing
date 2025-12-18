import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
df = pd.read_csv("train_samples_utf8.csv", encoding="utf-8-sig")
df["avg_salary_k"] = (df["salary_min_k"] + df["salary_max_k"]) / 2 # 统一用平均薪资

# 图1：公司规模分布
plt.figure(figsize=(7, 4))
plt.hist(df["company_size"].dropna(), bins=20)
plt.title("Distribution of Company Size (Long Tail)")
plt.xlabel("Company Size")
plt.ylabel("Count")
plt.tight_layout()
plt.show()

# 图2：薪资 vs 曝光 
tmp = df[["avg_salary_k", "impression_cnt"]].dropna().copy()

# 参考之前实习形式：用“分箱均值”画趋势线
tmp["salary_bin"] = pd.cut(tmp["avg_salary_k"], bins=10)
trend = tmp.groupby("salary_bin")["impression_cnt"].mean()
x_mid = [b.mid for b in trend.index]  # 每个箱子的中点
y_mean = trend.values

print("corr(avg_salary_k, impression_cnt) =", tmp["avg_salary_k"].corr(tmp["impression_cnt"])) #用这一步先查有没有正相关性，发现corr=0.38

plt.figure(figsize=(7, 4))
plt.scatter(tmp["avg_salary_k"], tmp["impression_cnt"], s=10)
plt.plot(x_mid, y_mean)  # 趋势线
plt.title("Relationship between Salary and Exposure")
plt.xlabel("Average Salary (k)")
plt.ylabel("Exposure / Impression Count")
plt.tight_layout()
plt.show()

# 图3：国际化 vs 非国际化曝光（箱线图）
non_intl = df.loc[df["intl_flag"] == 0, "impression_cnt"].dropna()
intl = df.loc[df["intl_flag"] == 1, "impression_cnt"].dropna()
print("Non-Intl mean/median:", float(non_intl.mean()), float(non_intl.median()))
print("Intl mean/median:", float(intl.mean()), float(intl.median()))

plt.figure(figsize=(7, 4))
plt.boxplot([non_intl, intl], labels=["Non-Intl Jobs", "Intl Jobs"])
plt.title("Exposure Comparison: Intl vs Non-Intl Jobs")
plt.ylabel("Exposure / Impression Count")
plt.tight_layout()
plt.show()

# 图4：价格标签分布
plt.figure(figsize=(7, 4))
plt.hist(df["price_label"].dropna(), bins=20)
plt.title("Distribution of Recommended Price Label")
plt.xlabel("Price Label")
plt.ylabel("Count")
plt.tight_layout()
plt.show()
