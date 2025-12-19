import hashlib
from fastapi import APIRouter, HTTPException
from app.schemas.pricing import PricingQuoteRequest, PricingQuoteResponse
from app.services.pricing_service import fetch_pricing_row, fallback_price

router = APIRouter(prefix="/api/pricing", tags=["pricing"])

def stable_bucket(key: str) -> str:
    h = hashlib.md5(key.encode("utf-8")).hexdigest()
    v = int(h[:8], 16)
    return "A" if (v % 2 == 0) else "B"

@router.post("/quote", response_model=PricingQuoteResponse)
def quote(req: PricingQuoteRequest):
    key = str(req.company_id or req.job_id or "anonymous")
    bucket = stable_bucket(key)

    row = fetch_pricing_row(req.job_id, req.company_id)
    if row is not None:
        if "price_label" not in row:
            raise HTTPException(status_code=500, detail=f"No 'price_label' column. keys={list(row.keys())}")
        return PricingQuoteResponse(
            model_version="baseline-db-v1",
            bucket=bucket,
            job_id=req.job_id or row.get("job_id"),
            company_id=req.company_id or row.get("company_id"),
            price=float(row["price_label"]),
            source="db",
            debug={"row_keys": list(row.keys())}
        )

    fb = fallback_price(req.expected_value, req.expected_high_quality_applies, req.brand_factor or 1.0)
    return PricingQuoteResponse(
        model_version="fallback-rule-v1",
        bucket=bucket,
        job_id=req.job_id,
        company_id=req.company_id,
        price=fb["price"],
        source="fallback",
        debug=fb["debug"]
    )
