import pandas as pd
import numpy as np

TRAIN_PATH = "train_samples.csv"
OUT_PATH = "job_events.csv"

CAP = 200 #选择200条cap，避免过大

np.random.seed(42)

df = pd.read_csv(TRAIN_PATH)

def pick_col(cands):
    for c in cands:
        if c in df.columns:
            return c
    raise ValueError(f"Missing required column. Tried: {cands}. Your columns: {list(df.columns)}")

col_job = pick_col(["job_id", "jobId", "jid"])
col_company = pick_col(["company_id", "companyId", "cid"])

col_imp = pick_col(["impression_cnt", "impression", "imp_cnt"])
col_view = pick_col(["view_cnt", "view", "pv_cnt"])
col_apply = pick_col(["apply_cnt", "apply", "apply_count"])
col_hire = pick_col(["hire_cnt", "hire", "hire_count"])

def to_int(x):
    try:
        return int(x)
    except:
        return 0

def cap(x):
    x = to_int(x)
    return max(0, min(x, CAP))

# 时间骨架：sample_dt / label_time
base = pd.Timestamp.today().normalize()

# 过去 30 天随机分布
df["sample_dt"] = (base - pd.to_timedelta(np.random.randint(0, 30, size=len(df)), unit="D")).dt.date
# label_time：当天随机小时（作为“label发生时刻”）
df["label_time"] = pd.to_datetime(df["sample_dt"].astype(str)) + pd.to_timedelta(
    np.random.randint(0, 24, size=len(df)), unit="h"
)

rows = []

for _, r in df.iterrows():
    job_id = str(r[col_job])
    company_id = str(r[col_company])
    label_time = pd.to_datetime(r["label_time"])
    start = label_time - pd.Timedelta(hours=24)  # 只用 label_time 之前 24h 的事件，避免“用未来”

    def gen(n, etype):
        if n <= 0:
            return
        # 随机 event_time 分布在 [start, label_time)
        ts = start + (label_time - start) * np.random.rand(n)
        uids = np.random.randint(1, 500000, size=n).astype(str)
        for t, u in zip(ts, uids):
            rows.append((pd.to_datetime(t).strftime("%Y-%m-%d %H:%M:%S"), etype, job_id, company_id, u))

    gen(cap(r[col_imp]), "impression")
    gen(cap(r[col_view]), "view")
    gen(cap(r[col_apply]), "apply")
    gen(cap(r[col_hire]), "hire")

events = pd.DataFrame(rows, columns=["event_time", "event_type", "job_id", "company_id", "user_id"])
events.to_csv(OUT_PATH, index=False)
print("sample:\n", events.head(5))
