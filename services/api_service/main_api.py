from fastapi import FastAPI
from api_service.routes import router
from prometheus_fastapi_instrumentator import Instrumentator

app = FastAPI(
    title="ClimaPulse API",
    description="Real-time Weather Data API",
    version="1.0"
)

app.include_router(router)
Instrumentator().instrument(app).expose(app)


@app.get("/")
def root():
    return {"message": "ClimaPulse API is running"}