from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from .api.v1.routes import router
import os

app = FastAPI()

# ★★★★★ ADD CORS HERE ★★★★★
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],            # allow all origins (React, Postman, mobile apps)
    allow_credentials=True,
    allow_methods=["*"],            # GET, POST, PUT, DELETE, OPTIONS
    allow_headers=["*"],            # Content-Type, Authorization, etc.
)

# Your routes
app.include_router(router, prefix="/api/v1")
