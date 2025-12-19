from fastapi import APIRouter, HTTPException
from backend.schemas import PriceResponse
from backend.services.job_pricing.service import get_price_by_job_id

router = APIRouter(prefix="/api/job_pricing", tags=["job_pricing"])

@router.get("/price/{job_id}", response_model=PriceResponse)
def get_price(job_id: int):
    row = get_price_by_job_id(job_id)
    if not row:
        raise HTTPException(status_code=404, detail=f"job_id={job_id} not found")
    return {
        "job_id": row["job_id"],
        "price_label": float(row["price_label"]),
        "expected_value": float(row["expected_value"]) if row.get("expected_value") is not None else None,
        "expected_high_quality_applies": float(row["expected_high_quality_applies"]) if row.get("expected_high_quality_applies") is not None else None,
        "source": "mysql",
    }
