from fastapi import FastAPI

app = FastAPI(title="HarmonicMesh API", version="0.1.0")


@app.get("/health")
def health():
    return {"status": "ok", "service": "harmonicmesh-api"}


# Phase 5: alert endpoints will be added here.
