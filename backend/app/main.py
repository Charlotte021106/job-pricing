import os
from typing import Optional

import pymysql
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

load_dotenv()

app = FastAPI(title="Job Pricing API", version="0.2.0")

# ---------- config ----------
MYSQL_HOST = os.getenv("MYSQL_HOST", "127.0.0.1")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "") #可以输密码
MYSQL_DB = os.getenv("MYSQL_DB", "job_pricing")
MYSQL_CHARSET = os.getenv("MYSQL_CHARSET", "utf8mb4")

PRICING_TABLE = os.getenv("PRICING_TABLE", "train_samples")
PRICE_COL = os.getenv("PRICE_COL", "price_label")


def get_conn():
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


@app.get("/health")
def health():
    return {"status": "ok"}


class QuoteReq(BaseModel):
    job_id: Optional[int] = None
    company_id: Optional[int] = None
    expected_value: Optional[float] = None
    expected_high_quality_applies: Optional[float] = None
    brand_factor: float = 1.0


@app.post("/api/pricing/quote")
def quote(req: QuoteReq):
    # 1) 优先 DB 查 price_label（按 AND 精确匹配）
    if req.job_id is not None or req.company_id is not None:
        where = []
        params = []
        if req.job_id is not None:
            where.append("job_id=%s")
            params.append(req.job_id)
        if req.company_id is not None:
            where.append("company_id=%s")
            params.append(req.company_id)

        sql = f"SELECT job_id, company_id, {PRICE_COL} AS price FROM {PRICING_TABLE} WHERE {' AND '.join(where)} LIMIT 1"

        try:
            conn = get_conn()
            with conn.cursor() as cur:
                cur.execute(sql, params)
                row = cur.fetchone()
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"MySQL query failed: {e}")
        finally:
            try:
                conn.close()
            except Exception:
                pass

        if row is not None and row.get("price") is not None:
            return {
                "model_version": "baseline-db-v1",
                "source": "db",
                "job_id": row.get("job_id"),
                "company_id": row.get("company_id"),
                "price": float(row["price"]),
                "debug": {"table": PRICING_TABLE, "price_col": PRICE_COL},
            }

    # 2) 查不到 -> fallback（保证永远有返回）
    ev = req.expected_value if req.expected_value is not None else 100.0
    ehq = req.expected_high_quality_applies if req.expected_high_quality_applies is not None else 1.0
    base = 0.6 * ev + 50.0 * ehq
    price = base * (req.brand_factor if req.brand_factor is not None else 1.0)
    price = max(120.0, min(650.0, price))

    return {
        "model_version": "fallback-rule-v1",
        "source": "fallback",
        "job_id": req.job_id,
        "company_id": req.company_id,
        "price": round(float(price), 2),
        "debug": {"expected_value": ev, "expected_high_quality_applies": ehq, "brand_factor": req.brand_factor, "base": round(float(base), 2)},
    }
