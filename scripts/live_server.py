import json
from collections import deque

import uvicorn
import yaml
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware

# Load config
with open("config/app_settings.yaml", "r") as f:
    config = yaml.safe_load(f)
    port = config["webui"]["servers"][0]["url"].split(":")[-1]
app = FastAPI()

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Store last 100 samples per run
buffers = {}


@app.get("/api/buffers")
async def list_buffers():
    return {
        "active_run_ids": list(buffers.keys()),
        "buffer_sizes": {str(k): len(v) for k, v in buffers.items()},
    }


@app.post("/api/update")
async def update_sample(request: Request):
    data = await request.json()
    run_id = data["run_id"]

    if run_id not in buffers:
        buffers[run_id] = deque(maxlen=100)

    buffers[run_id].append(data)
    return {"status": "ok"}


@app.get("/api/live/{run_id}")
async def get_live(run_id: int):
    if run_id not in buffers:
        return {"samples": []}
    return {"samples": list(buffers[run_id])}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8501)
