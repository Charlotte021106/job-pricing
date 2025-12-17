import os
import numpy as np
import pandas as pd

np.random.seed(42)

# 基础信息
n_companies = 500 # 参考了行业报告，中型招聘平台的活跃企业数大概在这个量级
jobs_per_company_min = 1
jobs_per_company_max = 5
base_lambda_exposure = 800

# 行业列表参考了boss2021年应届生就业趋势报告里行业关注分布
industries = ["互联网", "金融", "制造业", "教育", "咨询", "零售"]
# 融资阶段划分参考了IT 桔子 2023 医疗健康行业投融资报告
funding_stages = ["未融资", "天使轮","种子轮", "A轮", "B轮", "C轮及以上", "上市公司"]
cities = ["北京", "上海", "深圳", "杭州", "新加坡", "广州"]
# 偏向考虑技术类岗位
job_functions = ["算法工程师", "数据分析", "后端开发", "前端开发", "产品经理", "运营"]


def min_max(s: pd.Series) -> pd.Series:
    # 防止分母为0
    return (s - s.min()) / (s.max() - s.min() + 1e-5)


# 1. 企业表

# 企业基础信息
company_ids = np.arange(1, n_companies + 1)

company_profile = pd.DataFrame({
    "company_id": company_ids,
    "industry": np.random.choice(industries, size=n_companies),
    "company_size": np.random.choice([50, 100, 200, 500, 1000, 3000], size=n_companies),
    "funding_stage": np.random.choice(funding_stages, size=n_companies),
    "brand_level": np.random.randint(1, 6, size=n_companies),
    "salary_level": np.random.randint(1, 6, size=n_companies),
    "intl_flag": np.random.binomial(1, 0.3, size=n_companies),
    "top10_talent_ratio": np.round(np.random.uniform(0.05, 0.5, size=n_companies), 3),
    "avg_apply_cnt": np.random.randint(50, 300, size=n_companies),
    "hire_rate": np.round(np.random.uniform(0.05, 0.3, size=n_companies), 3)
})

# 企业软指标
company_soft_metrics = pd.DataFrame({
    "company_id": company_ids,
    "overall_rating": np.round(np.random.uniform(3.0, 5.0, size=n_companies), 2),
    "work_life_balance_score": np.round(np.random.uniform(2.5, 5.0, size=n_companies), 2),
    "salary_competitiveness": np.round(np.random.uniform(2.5, 5.0, size=n_companies), 2),
    "resume_response_rate": np.round(np.random.uniform(0.2, 0.9, size=n_companies), 3),
    "resume_response_time": np.round(np.random.uniform(0.5, 7.0, size=n_companies), 2),
    "offer_conversion_rate": np.round(np.random.uniform(0.1, 0.6, size=n_companies), 3)
})

# 指标归一化，计算综合软指标得分
soft_norm = pd.DataFrame({
    "overall_rating_n": min_max(company_soft_metrics["overall_rating"]),
    "wlb_n": min_max(company_soft_metrics["work_life_balance_score"]),
    "salary_comp_n": min_max(company_soft_metrics["salary_competitiveness"]),
    "resp_rate_n": min_max(company_soft_metrics["resume_response_rate"]),
    "resp_time_n": 1 - min_max(company_soft_metrics["resume_response_time"]),  # 响应时间越短越好，所以取反
    "offer_conv_n": min_max(company_soft_metrics["offer_conversion_rate"])
})

# 权重分配上，自己考虑整体评分和工作生活平衡权重最高（各20%）
talent_care_index = (0.2 * soft_norm["overall_rating_n"]
                     + 0.2 * soft_norm["wlb_n"]
                     + 0.15 * soft_norm["salary_comp_n"]
                     + 0.2 * soft_norm["resp_rate_n"]
                     + 0.1 * soft_norm["resp_time_n"]
                     + 0.15 * soft_norm["offer_conv_n"])

company_tier_features = pd.DataFrame({
    "company_id": company_ids,
    "talent_care_index": np.round(talent_care_index, 3)
})

# 公司分三档：T1/T2/T3
tier_score = company_profile["brand_level"] + 2 * company_tier_features["talent_care_index"]
q1 = tier_score.quantile(0.33)
q2 = tier_score.quantile(0.66)

def assign_tier(x):
    if x <= q1:
        return "T3"
    if x <= q2:
        return "T2"
    return "T1"

company_tier_features["enterprise_tier"] = tier_score.apply(assign_tier)


# 2. 岗位表 + 统计表 + 定价标签
# 岗位表
job_rows = []
job_id = 1

for cid in company_ids:
    n_jobs = np.random.randint(jobs_per_company_min, jobs_per_company_max + 1)
    
    for _ in range(n_jobs):
        func = np.random.choice(job_functions)

        # 判断是否热门岗位：后续可根据近期简历增长率top3更换
        if func in ["算法工程师", "数据分析", "后端开发"]:
            hot_function = 1
        else:
            hot_function = 0

        level = np.random.choice(["初级", "中级", "高级"])

        # 基础薪资范围8-40k，参考了BOSS直聘的薪资数据，初级岗多在8-20k
        base_salary = np.random.randint(8, 40)
        salary_min_k = int(base_salary)
        salary_max_k = int(base_salary + np.random.randint(2, 15)) # 用base随机加个差值做上限

        job_rows.append({
            "job_id": job_id,
            "company_id": cid,
            "job_type": "全职", # 目前先考虑全职
            "job_level": level,
            "job_function": func,
            "city": np.random.choice(cities),
            "salary_min_k": int(base_salary),
            "salary_max_k": int(base_salary + np.random.randint(2, 15)),
            "target_top10": int(np.random.binomial(1, 0.4)),
            "online_days": int(np.random.randint(7, 60)),# 超过60天下架
            "urgency": int(np.random.choice([1, 2, 3])),
            "hot_function": int(hot_function)
        })
        
        job_id += 1

job_profile = pd.DataFrame(job_rows)

# 拼接企业信息
job_merged = job_profile.merge(company_profile, on="company_id", how="left")
job_merged = job_merged.merge(company_tier_features, on="company_id", how="left")

# 生成曝光-查看-投递-录用四步骤
eps = np.random.normal(0, 0.8, size=len(job_merged)) # 做一点噪声

# 各因素权重：品牌和薪资对吸引力影响最大（经历判断），国际业务权重稍低
w_brand = 0.8
w_salary = 0.7
w_intl = 0.5
w_top10 = 0.9
w_hot = 0.6  # 后期可调参

attract_score = (w_brand * job_merged["brand_level"]
                 + w_salary * job_merged["salary_level"]
                 + w_intl * job_merged["intl_flag"]
                 + w_top10 * job_merged["target_top10"]
                 + w_hot * job_merged["hot_function"]
                 + eps)

p_view = 1 / (1 + np.exp(-attract_score)) # sigmoid：让分数转换为0-1的概率

# 曝光数：用Poisson模拟
lam = base_lambda_exposure * p_view
lam = lam.clip(20, None) #避免全是0，设定最小值20

impressions = np.random.poisson(lam=lam)

# 查看数：从曝光里按p_view概率抽出
p_view_clip = p_view.clip(0.05, 0.95)
views = np.random.binomial(n=impressions, p=p_view_clip) # binomial

# 投递数：设置和p_view成正比的投递概率
p_apply = (p_view * 0.3).clip(0.02, 0.6)
applies = np.random.binomial(n=views, p=p_apply)

# 录用数：使用公司hire_rate从投递里面抽出
hire_rate = job_merged["hire_rate"].fillna(0.1).values
hire_rate = np.clip(hire_rate, 0.02, 0.8)
hires = np.random.binomial(n=applies, p=hire_rate)

job_stats = pd.DataFrame({
    "job_id": job_profile["job_id"],
    "impression_cnt": impressions,
    "view_cnt": views,
    "apply_cnt": applies,
    "hire_cnt": hires
})

# 设置定价标签

top10_ratio = job_merged["top10_talent_ratio"].fillna(0.1).values
expected_applies = applies * top10_ratio # 预估一个高质量投递数

v = 100
ROI_target = 3.0 #目标ROI，可根据实际调整
expected_value = v * expected_applies # 预估一个岗位的价值
price_base = expected_value / ROI_target #转换为基础价格

price_log = np.log1p(price_base) # log压缩：防止少数岗位把价格拉的太高

q_low = np.quantile(price_log, 0.05)
q_high = np.quantile(price_log, 0.95) # 去掉5%以下和95%以上的极端值，让价格更合理

scaled = (price_log - q_low) / (q_high - q_low + 1e-5)
scaled = np.clip(scaled, 0, 1)

price_core = 250 + scaled * (380 - 250)

# 品牌溢价：考虑品牌等级高报价高
brand_level = job_merged["brand_level"].values
brand_factor = 1 + 0.05 * (brand_level - 3)

price_label = price_core * brand_factor
price_label = np.clip(price_label, 120, 650) #做一个范围限制

job_pricing_label = pd.DataFrame({
    "job_id": job_profile["job_id"],
    "expected_high_quality_applies": expected_applies.round(2),
    "expected_value": expected_value.round(2),
    "price_label": price_label.round(2),
})

print(job_pricing_label["price_label"].describe()) #可看价格大概分布

# 3.行为日志

logs = []
log_id = 1
top10_ratio_by_job = {}

# 先缓存每个岗位的头部人才比例，避免循环里重复计算
for i in range(len(job_profile)):
    jid = int(job_profile.loc[i, "job_id"])
    top10_ratio_by_job[jid] = float(top10_ratio[i])
    

# 遍历每个岗位
for _, row in job_stats.iterrows():
    jid = int(row["job_id"])
    n_view = int(row["view_cnt"])
    n_apply = int(row["apply_cnt"])
    n_hire = int(row["hire_cnt"])

    # 没有任何行为就跳过
    if n_view == 0 :
        continue

    # 生成看过的人和看过的时间
    top10_p = float(top10_ratio_by_job.get(jid, 0.1))
    # 高质量用户比例：基础15% + 头部人才占比*1.2，这个公式是试出来的，效果比较合理
    p_hq = float(np.clip(0.15 + 1.2 * top10_p, 0.15, 0.75)) # top10 越高，高质量用户比例越高

    viewer_ids = [f"U_{jid}_{k+1}" for k in range(n_view)]
    base_times = pd.Timestamp("2026-01-01") + pd.to_timedelta(
        np.random.randint(0, 60, size=n_view), unit="D"
    )
    is_hq = np.random.binomial(1, p_hq, size=n_view)

    # 生成view日志    
    for uid, t, hq in zip(viewer_ids, base_times, is_hq):
        logs.append({            
             "log_id": log_id,            
             "job_id": jid,            
             "user_id": uid,            
             "event_time": t,            
             "event_type": "view",            
             "is_high_quality_user": int(hq),            
             "apply_result": None        
        })
        log_id += 1
        
    # 生成apply日志（考虑从view里抽取一部分）    
    if n_apply > 0:
         apply_idx = np.random.choice(n_view, size=n_apply, replace=False)
         
         for idx in apply_idx:
             logs.append({                 
                 "log_id": log_id,                 
                 "job_id": jid,                 
                 "user_id": viewer_ids[idx],                 
                 "event_time": base_times[idx] + pd.to_timedelta(np.random.randint(0, 3), unit="D"),                 
                 "event_type": "apply",                 
                 "is_high_quality_user": int(is_hq[idx]),                 
                 "apply_result": None              
              })
             log_id += 1
             
    # 生成offer/reject（考虑从apply抽取一部分offer）

    if n_hire > 0:
        offer_idx = np.random.choice(apply_idx, size=n_hire, replace=False)
        offer_set = set(offer_idx.tolist())
    else:
        offer_set = set()

    for idx in apply_idx:
        result = "offer" if idx in offer_set else "reject"
        logs.append({
            "log_id": log_id,
            "job_id": jid,
            "user_id": viewer_ids[idx],
            "event_time": base_times[idx] + pd.to_timedelta(np.random.randint(3, 15), unit="D"),
            "event_type": result,
            "is_high_quality_user": int(is_hq[idx]),
            "apply_result": result
        })
        log_id += 1

job_apply_logs = pd.DataFrame(logs)

# 打印表
print("company_profile:", company_profile.shape)
print("company_soft_metrics:", company_soft_metrics.shape)
print("company_tier_features:", company_tier_features.shape)
print("job_profile:", job_profile.shape)
print("job_stats:", job_stats.shape)
print("job_pricing_label:", job_pricing_label.shape)
print("job_apply_logs:", job_apply_logs.shape)

