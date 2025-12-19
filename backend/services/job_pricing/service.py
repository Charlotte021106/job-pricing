from backend.database import get_conn

def get_price_by_job_id(job_id: int) -> dict | None:
    sql = """
    SELECT job_id, price_label, expected_value, expected_high_quality_applies
    FROM job_pricing_label
    WHERE job_id = %s
    LIMIT 1
    """
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (job_id,))
            row = cur.fetchone()
            return row
    finally:
        conn.close()
