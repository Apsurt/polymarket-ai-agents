from fastapi import FastAPI
from app.core.config import settings
import redis

app = FastAPI(title="Polymarket AI Trader", version="0.1.0")

@app.get("/")
async def root():
    return {"message": "Polymarket AI Trader API", "status": "running"}

@app.get("/health")
async def health_check():
    try:
        # Test Redis connection
        r = redis.Redis(host=settings.REDIS_HOST, port=settings.REDIS_PORT)
        r.ping()
        redis_status = "connected"
    except Exception as e:
        redis_status = f"error: {str(e)}"

    return {
        "status": "healthy",
        "redis": redis_status,
        "database_url": settings.DATABASE_URL[:20] + "..." if settings.DATABASE_URL else "not configured"
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
