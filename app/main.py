from fastapi import FastAPI
from app.core.config import settings

app = FastAPI(title="Polymarket AI Trader", version="0.1.0")

@app.get("/")
async def root():
    return {"message": "Polymarket AI Trader API", "status": "running"}

@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "database_url": settings.DATABASE_URL[:20] + "..." if settings.DATABASE_URL else "not configured"
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
