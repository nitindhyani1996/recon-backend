from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from .api.v1.routes import router
import os

app = FastAPI()

# Configure CORS
allowed_origins = [
    "https://ai-recon.vercel.app",  # Your frontend
    "http://localhost:3000",         # Local development
    "http://localhost:5173",         # Vite dev server
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(router, prefix="/api/v1")
