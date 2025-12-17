CREATE DATABASE job_pricing;
USE job_pricing;

-- 企业基础画像
CREATE TABLE company_profile (
    company_id              INT PRIMARY KEY,
    industry                VARCHAR(32),
    company_size            INT,
    funding_stage           VARCHAR(32),
    brand_level             TINYINT,
    salary_level            TINYINT,
    intl_flag               TINYINT,
    top10_talent_ratio      DECIMAL(5,3),
    avg_apply_cnt           INT,
    hire_rate               DECIMAL(5,3)
) ;

-- 岗位软性质指标
CREATE TABLE company_soft_metrics (
    company_id              INT PRIMARY KEY,
    overall_rating          DECIMAL(3,2),
    work_life_balance_score DECIMAL(3,2),
    salary_competitiveness  DECIMAL(3,2),
    resume_response_rate    DECIMAL(5,3),
    resume_response_time    DECIMAL(5,2),
    offer_conversion_rate   DECIMAL(5,3),
    FOREIGN KEY (company_id) REFERENCES company_profile(company_id)
);

-- 企业分层
CREATE TABLE company_tier_features (
    company_id          INT PRIMARY KEY,
    talent_care_index   DECIMAL(5,3),
    enterprise_tier     VARCHAR(8),
    FOREIGN KEY (company_id) REFERENCES company_profile(company_id)
);

-- 岗位基础信息
CREATE TABLE job_profile (
    job_id          INT PRIMARY KEY,
    company_id      INT NOT NULL,
    job_type        VARCHAR(16),
    job_level       VARCHAR(16),
    job_function    VARCHAR(32),
    city            VARCHAR(32),
    salary_min_k    INT,
    salary_max_k    INT,
    target_top10    TINYINT,
    online_days     INT,
    urgency         TINYINT,
    hot_function    TINYINT,
    FOREIGN KEY (company_id) REFERENCES company_profile(company_id)
);

-- 岗位统计指标
CREATE TABLE job_stats (
    job_id          INT PRIMARY KEY,
    impression_cnt  INT,
    view_cnt        INT,
    apply_cnt       INT,
    hire_cnt        INT,
    FOREIGN KEY (job_id) REFERENCES job_profile(job_id)
);

-- 定价标签
CREATE TABLE job_pricing_label (
    job_id                      INT PRIMARY KEY,
    expected_high_quality_applies DECIMAL(10,2),
    expected_value              DECIMAL(12,2),
    price_label                 DECIMAL(10,2),
    FOREIGN KEY (job_id) REFERENCES job_profile(job_id)
);

-- 行为日志（关于序列数据）
CREATE TABLE job_apply_logs (
    log_id              BIGINT PRIMARY KEY,
    job_id              INT NOT NULL,
    user_id             VARCHAR(64),
    event_time          DATETIME,
    event_type          VARCHAR(16),
    is_high_quality_user TINYINT,
    apply_result        VARCHAR(16),
    FOREIGN KEY (job_id) REFERENCES job_profile(job_id),
    INDEX idx_job_time (job_id, event_time)
);
