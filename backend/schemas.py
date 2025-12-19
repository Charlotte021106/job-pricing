from pydantic import BaseModel
from typing import Optional

class PriceResponse(BaseModel):
    job_id: int
    price_label: float
    expected_value: Optional[float] = None
    expected_high_quality_applies: Optional[float] = None
    source: str = "mysql"
