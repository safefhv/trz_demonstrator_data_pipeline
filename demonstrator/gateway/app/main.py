"""HTTP API + static web UI for the OPC UA -> MQTT gateway."""

import logging
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from asyncua import ua

from .gateway import Gateway

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logging.getLogger("asyncua").setLevel(logging.WARNING)

STATIC = Path(__file__).parent / "static"
gateway = Gateway()


@asynccontextmanager
async def lifespan(app):
    await gateway.start()
    yield
    await gateway.shutdown()


app = FastAPI(title="OPC UA → MQTT Gateway", lifespan=lifespan)
app.mount("/static", StaticFiles(directory=STATIC), name="static")


@app.exception_handler(ua.UaError)
async def ua_error(request, exc):
    return JSONResponse(status_code=400, content={"detail": f"{type(exc).__name__}: {exc}"})


def bad_request(exc):
    return HTTPException(status_code=400, detail=str(exc) or type(exc).__name__)


class Settings(BaseModel):
    opcua_url: str | None = None
    mqtt_host: str | None = None
    mqtt_port: int | None = None
    topic_root: str | None = None
    machine_name: str | None = None
    publishing_interval_ms: int | None = None
    qos: int | None = None
    retain: bool | None = None


class ConnectRequest(BaseModel):
    url: str


class Tag(BaseModel):
    node_id: str
    description: str | None = None
    display_name: str | None = None


class MethodCall(BaseModel):
    object_id: str
    method_id: str
    args: list[Any] = []


class WriteRequest(BaseModel):
    node_id: str
    value: Any


@app.get("/")
async def index():
    return FileResponse(STATIC / "index.html")


@app.get("/api/state")
async def state():
    return gateway.state()


@app.put("/api/settings")
async def update_settings(settings: Settings):
    await gateway.update_settings(settings.model_dump(exclude_none=True))
    return gateway.state()


@app.post("/api/opcua/connect")
async def connect(req: ConnectRequest):
    await gateway.connect(req.url.strip())
    return gateway.state()


@app.post("/api/opcua/disconnect")
async def disconnect():
    await gateway.disconnect()
    return gateway.state()


@app.get("/api/browse")
async def browse(node_id: str | None = None):
    try:
        return await gateway.browse(node_id)
    except Exception as exc:
        raise bad_request(exc)


@app.get("/api/node")
async def node(node_id: str):
    try:
        return await gateway.node_details(node_id)
    except Exception as exc:
        raise bad_request(exc)


@app.put("/api/tags")
async def set_tags(tags: list[Tag]):
    await gateway.set_tags([t.model_dump() for t in tags])
    return gateway.state()


@app.post("/api/forwarding/start")
async def start_forwarding():
    try:
        await gateway.start_forwarding()
    except Exception as exc:
        raise bad_request(exc)
    return gateway.state()


@app.post("/api/forwarding/stop")
async def stop_forwarding():
    await gateway.stop_forwarding()
    return gateway.state()


@app.post("/api/method")
async def call_method(call: MethodCall):
    try:
        return {"result": await gateway.call_method(call.object_id, call.method_id, call.args)}
    except Exception as exc:
        raise bad_request(exc)


@app.post("/api/write")
async def write(req: WriteRequest):
    try:
        await gateway.write_value(req.node_id, req.value)
    except Exception as exc:
        raise bad_request(exc)
    return {"ok": True}


@app.get("/api/config")
async def export_config():
    """Selection in the format of ../opc_ua_client/config.json (plus gateway settings)."""
    cfg = gateway.config
    return JSONResponse(
        {
            "machine_name": cfg["machine_name"],
            "opcua_url": cfg["opcua_url"],
            "topic_root": cfg["topic_root"],
            "subscription_list": [{"node_id": t["node_id"], "description": t["description"]}
                                  for t in cfg["subscription_list"]],
        },
        headers={"Content-Disposition": f'attachment; filename="{cfg["machine_name"]}.json"'},
    )
