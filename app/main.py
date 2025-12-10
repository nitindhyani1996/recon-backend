from fastapi import FastAPI
from .api.v1.routes import router
import os

app = FastAPI()

app.include_router(router, prefix="/api/v1")
