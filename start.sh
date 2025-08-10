#!/bin/bash
# Install dependencies
pip install --no-cache-dir -r requirements.txt

# Start FastAPI app with uvicorn
uvicorn app:app --host 0.0.0.0 --port 10000

