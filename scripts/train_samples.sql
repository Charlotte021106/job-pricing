USE job_pricing;

DROP TABLE IF EXISTS train_samples;

CREATE TABLE train_samples AS
SELECT
  j.job_id,
  j.company_id,
  j.job_level,
  j.job_function,
  j.city,
  j.salary_min_k,
  j.salary_max_k,
  j.target_top10,
  j.online_days,
  j.urgency,
  j.hot_function,

  c.industry,
  c.company_size,
  c.funding_stage,
  c.brand_level,
  c.salary_level,
  c.intl_flag,
  c.top10_talent_ratio,
  c.avg_apply_cnt,
  c.hire_rate,

  t.talent_care_index,
  t.enterprise_tier,

  s.overall_rating,
  s.work_life_balance_score,
  s.salary_competitiveness,
  s.resume_response_rate,
  s.resume_response_time,
  s.offer_conversion_rate,

  st.impression_cnt,
  st.view_cnt,
  st.apply_cnt,
  st.hire_cnt,

  p.price_label
  
FROM job_profile j
JOIN company_profile c ON j.company_id = c.company_id
JOIN company_tier_features t ON j.company_id = t.company_id
JOIN company_soft_metrics s ON j.company_id = s.company_id
JOIN job_stats st ON j.job_id = st.job_id
JOIN job_pricing_label p ON j.job_id = p.job_id;
