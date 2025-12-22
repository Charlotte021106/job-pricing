This dataset represents offline training samples for job pricing, aggregated at the job level.

# Primary Identifiers
job_id: Unique identifier of a job posting
company_id: Unique identifier of the company

# Job & Company Attributes
job_level
job_function
city
industry
company_size
funding_stage
brand_level
enterprise_tier
intl_flag
salary_level

# Behavior Features (Aggregated)
These fields represent aggregated user behavior signals over a recent time window (e.g. 1 day/7 days):

impression_cnt: number of job impressions
view_cnt: number of job detail views
apply_cnt: number of job applications
hire_cnt: number of successful hires

# Conversion & Quality Indicators
resume_response_rate
resume_response_time
offer_conversion_rate
avg_apply_cnt
top10_talent_ratio
talent_care_index

# Label
price_label: target pricing label used for model training

# Notes
This table is treated as an offline training snapshot.
In production, behavior-related fields are expected to be derived from raw event logs using ClickHouse aggregations.
