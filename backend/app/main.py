import os
import csv
import json
import math
import pymysql
from datetime import datetime
from dotenv import load_dotenv
from fastapi import FastAPI
from pydantic import BaseModel
from urllib.parse import quote as urlquote
from urllib.request import Request, urlopen

load_dotenv()
app = FastAPI(title="Job Pricing API", version="0.4.3")

# MySQL config (baseline) 
MYSQL_HOST = os.getenv("MYSQL_HOST", "127.0.0.1")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "") #可输入密码
MYSQL_DB = os.getenv("MYSQL_DB", "job_pricing")
MYSQL_CHARSET = os.getenv("MYSQL_CHARSET", "utf8mb4")

PRICING_TABLE = os.getenv("PRICING_TABLE", "train_samples")
PRICE_COL = os.getenv("PRICE_COL", "price_label")

# ClickHouse config (HTTP) 
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "127.0.0.1")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_DB = os.getenv("CLICKHOUSE_DB", "pricing")
CH_FEATURE_TABLE = os.getenv("CH_FEATURE_TABLE", "job_features_1d")
CLICKHOUSE_TIMEOUT = int(os.getenv("CLICKHOUSE_TIMEOUT", "5"))

# Local CSV fallback 
JOB_FEATURES_CSV = os.getenv(
    "JOB_FEATURES_CSV",
    os.path.join(os.getcwd(), "backend", "data", "job_features_1d.csv"),
)

FEATURE_STORE_MODE = os.getenv("FEATURE_STORE_MODE", "auto").lower().strip()

# 设置安全转 float 
def safe_float(value):
    if value is None:
        return 0.0
    text = str(value).strip()
    if text == "" or text.lower() == "nan":
        return 0.0
    try:
        return float(text)
    except Exception:
        return 0.0
        
def parse_datetime_string(text):
    text = (text or "").strip()
    formats = ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S")
    for fmt in formats:
        try:
            return datetime.strptime(text, fmt)
        except Exception:
            pass
    return None

# MySQL baseline：读取 train_samples 里的 price_label
def create_mysql_connection():
    return pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DB,
        charset=MYSQL_CHARSET,
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True,
    )

def get_baseline_price_from_mysql(job_id=None, company_id=None):
    """return: baseline_row, baseline_price, baseline_query, baseline_error"""
    if job_id is None and company_id is None:
        return None, None, None, None

    conditions = []
    params = []

    if job_id is not None:
        conditions.append("job_id=%s")
        params.append(job_id)

    if company_id is not None:
        conditions.append("company_id=%s")
        params.append(company_id)

    baseline_query = (
        f"SELECT job_id, company_id, {PRICE_COL} AS price "
        f"FROM {PRICING_TABLE} WHERE {' AND '.join(conditions)} LIMIT 1"
    )
    
    connection = None
    try:
        connection = create_mysql_connection()
        with connection.cursor() as cursor:
            cursor.execute(baseline_query, params)
            baseline_row = cursor.fetchone()

        if baseline_row and baseline_row.get("price") is not None:
            return baseline_row, float(baseline_row["price"]), baseline_query, None

        return baseline_row, None, baseline_query, None

    except Exception as e:
        return None, None, baseline_query, repr(e)

    finally:
        try:
            if connection:
                connection.close()
        except Exception:
            pass

# ClickHouse：HTTP 查询 + 读取 job_features_1d
def run_clickhouse_query(sql):
    url = (
        f"http://{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}/"
        f"?database={urlquote(CLICKHOUSE_DB)}&query={urlquote(sql)}"
    )
    request = Request(url, method="GET")
    with urlopen(request, timeout=CLICKHOUSE_TIMEOUT) as response:
        return response.read().decode("utf-8")


def build_clickhouse_feature_query(job_id: int) -> str:
    return f"""
    SELECT
        toString(dt) AS dt,
        job_id,
        impression_1d,
        view_1d,
        apply_1d,
        hire_1d
    FROM {CH_FEATURE_TABLE}
    WHERE job_id = {int(job_id)}
    ORDER BY dt DESC
    LIMIT 1
    FORMAT JSON
    """.strip()

def get_features_from_clickhouse(job_id: int):
    """return: job_features, feature_query, feature_error"""
    feature_query = build_clickhouse_feature_query(job_id)
    try:
        raw_result = run_clickhouse_query(feature_query)
        result_json = json.loads(raw_result)
        rows = (result_json or {}).get("data", [])
        if not rows:
            return None, feature_query, "no_rows"

        row = rows[0]
        job_features = {
            "dt": str(row.get("dt", "")),
            "job_id": int(row.get("job_id", job_id)),
            "impression_1d": safe_float(row.get("impression_1d")),
            "view_1d": safe_float(row.get("view_1d")),
            "apply_1d": safe_float(row.get("apply_1d")),
            "hire_1d": safe_float(row.get("hire_1d")),
        }
        return job_features, feature_query, None

    except Exception as e:
        return None, feature_query, repr(e)

# CSV：读取 job_features_1d.csv（按 job_id 找最新 dt）
def get_features_from_csv(job_id: int):
    """return: job_features, pseudo_query, feature_error"""
    csv_path = JOB_FEATURES_CSV
    pseudo_query = f"LOCAL_CSV({csv_path}) WHERE job_id={int(job_id)} ORDER BY dt DESC LIMIT 1"

    try:
        latest_features = None
        latest_time = None

        with open(csv_path, "r", encoding="utf-8") as file:
            reader = csv.DictReader(file)

            for row in reader:
                # job_id 不匹配就跳过
                try:
                    if int(row.get("job_id", -1)) != int(job_id):
                        continue
                except Exception:
                    continue

                dt_str = (row.get("dt") or "").strip()
                dt_val = parse_datetime_string(dt_str) or datetime.min

                if latest_features is None or dt_val > latest_time:
                    latest_time = dt_val
                    latest_features = {
                        "dt": dt_str,
                        "job_id": int(job_id),
                        "impression_1d": safe_float(row.get("impression_1d")),
                        "view_1d": safe_float(row.get("view_1d")),
                        "apply_1d": safe_float(row.get("apply_1d")),
                        "hire_1d": safe_float(row.get("hire_1d")),
                    }

        if latest_features is None:
            return None, pseudo_query, f"csv_no_row_for_job_id: {job_id}"

        return latest_features, pseudo_query, None

    except FileNotFoundError:
        return None, pseudo_query, f"csv_not_found: {csv_path}"
    except Exception as e:
        return None, pseudo_query, f"csv_read_failed: {repr(e)}"

# 统一入口：拿到 job_features（clickhouse 优先，不行就 csv）
def get_job_features(job_id: int):
    """
    return: job_features, feature_source, feature_query, feature_error
    """
    mode = FEATURE_STORE_MODE

    if mode == "off":
        return None, "off", None, None

    if mode == "local_csv":
        job_features, feature_query, feature_error = get_features_from_csv(job_id)
        return job_features, "local_csv", feature_query, feature_error

    if mode == "clickhouse":
        job_features, feature_query, feature_error = get_features_from_clickhouse(job_id)
        return job_features, "clickhouse", feature_query, feature_error

    # 先 ClickHouse
    job_features, feature_query, feature_error = get_features_from_clickhouse(job_id)
    if job_features is not None and feature_error is None:
        return job_features, "clickhouse", feature_query, None

    # ClickHouse 不行 -> CSV
    csv_features, csv_query, csv_error = get_features_from_csv(job_id)

    combined_error = None
    if feature_error:
        combined_error = f"clickhouse_error: {feature_error}"
    if csv_error:
        combined_error = (combined_error + " | " if combined_error else "") + f"csv_error: {csv_error}"

    return csv_features, "local_csv_fallback", csv_query, combined_error

# Pricing Logic 
def pricing_by_features(
    features: dict,
    brand_level: float = 3.0,
    top_talent_ratio: float = 0.10,
    roi_target: float = 3.0,
    v: float = 100.0,
) -> float:
    apply_cnt = float(features.get("apply_1d", 0.0))
    top_talent_ratio = max(0.0, min(float(top_talent_ratio), 1.0))

    expected_applies = apply_cnt * top_talent_ratio
    expected_value = float(v) * expected_applies

    price_base = expected_value / max(float(roi_target), 1e-6)

    z = math.log1p(max(price_base, 0.0))
    z_min, z_max = 0.0, 6.0
    z_norm = max(0.0, min(1.0, (z - z_min) / (z_max - z_min)))
    price_core = 250.0 + z_norm * (380.0 - 250.0)

    brand_factor = 1.0 + 0.05 * (float(brand_level) - 3.0)
    price_label = price_core * brand_factor

    price_label = max(120.0, min(650.0, price_label))
    return round(float(price_label), 2)

# API
@app.get("/health")
def health():
    return {
        "status": "ok",
        "feature_store_mode": FEATURE_STORE_MODE,
        "job_features_csv": JOB_FEATURES_CSV,
        "clickhouse_http": f"{CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}",
        "clickhouse_db": CLICKHOUSE_DB,
        "ch_table": CH_FEATURE_TABLE,
    }

class QuoteReq(BaseModel):
    job_id: int | None = None
    company_id: int | None = None

    brand_factor: float = 1.0
    brand_level: float | None = None
    top_talent_ratio: float = 0.10
    roi_target: float = 3.0
    v: float = 100.0

    expected_value: float | None = None
    expected_high_quality_applies: float | None = None

@app.post("/api/pricing/quote")
def quote(req: QuoteReq):
    # 1) 先查 MySQL baseline
    baseline_row, baseline_price, baseline_query, baseline_error = get_baseline_price_from_mysql(
        job_id=req.job_id, company_id=req.company_id
    )

    # 2) 再查特征并尝试算在线价（ClickHouse 优先，失败走 CSV）
    job_features = None
    feature_based_price = None

    if req.job_id is not None:
        job_features, _, _, _ = get_job_features(int(req.job_id))

        if job_features is not None:
            # brand_level：优先用传的；没有就用 brand_factor 反推
            if req.brand_level is not None:
                brand_level_value = float(req.brand_level)
            else:
                brand_factor_value = float(req.brand_factor if req.brand_factor is not None else 1.0)
                brand_level_value = 3.0 + (brand_factor_value - 1.0) / 0.05

            feature_based_price = pricing_by_features(
                job_features,
                brand_level=brand_level_value,
                top_talent_ratio=req.top_talent_ratio,
                roi_target=req.roi_target,
                v=req.v,
            )

    # 3) 有在线价：返回在线价 + baseline_price（对照）
    if feature_based_price is not None:
        return {
            "job_id": req.job_id,
            "company_id": req.company_id,
            "price": feature_based_price,
            "baseline_price": baseline_price,  
        }

    # 4) 没在线价但有 baseline：就用 baseline 当最终 price
    if baseline_price is not None:
        return {
            "job_id": req.job_id,
            "company_id": req.company_id,
            "price": baseline_price,
            "baseline_price": baseline_price,
        }

    # 5) 最后都失败：原 fallback 
    expected_value = req.expected_value if req.expected_value is not None else 100.0
    expected_high_quality_applies = (
        req.expected_high_quality_applies if req.expected_high_quality_applies is not None else 1.0
    )
    base = 0.6 * float(expected_value) + 50.0 * float(expected_high_quality_applies)
    fallback_price = base * float(req.brand_factor if req.brand_factor is not None else 1.0)
    fallback_price = max(120.0, min(650.0, fallback_price))

    return {
        "job_id": req.job_id,
        "company_id": req.company_id,
        "price": round(float(fallback_price), 2),
        "baseline_price": None,  
    }
