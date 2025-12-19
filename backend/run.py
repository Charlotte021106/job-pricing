from fastapi import FastAPI
from backend.routers.job_pricing import router as job_pricing_router

app = FastAPI(title="Job Pricing API", version="0.1.0")

@app.get("/health")
def health():
    return {"status": "ok"}

app.include_router(job_pricing_router)
