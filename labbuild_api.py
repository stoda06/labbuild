from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import logging
from labbuild import setup_environment, manage_environment, teardown_environment

app = FastAPI()

class SetupRequest(BaseModel):
    vendor: str
    course: str
    class_number: str = None
    start_pod: int
    end_pod: int
    component: str = None
    host: str
    datastore: str = "vms"
    thread: int = 4
    re_build: bool = False
    verbose: bool = False
    quiet: bool = False
    memory: int = None
    full: bool = False
    clonefrom: bool = False
    tag: str = "untagged"
    monitor_only: bool = False

class ManageRequest(BaseModel):
    vendor: str
    course: str
    class_number: str = None
    component: str = None
    start_pod: int
    end_pod: int
    host: str
    operation: str
    tag: str = "untagged"
    verbose: bool = False

class TeardownRequest(BaseModel):
    vendor: str
    course: str
    class_number: str = None
    start_pod: int
    end_pod: int
    host: str
    tag: str = "untagged"
    verbose: bool = False
    monitor_only: bool = False

class ArgsWrapper:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

@app.post("/setup")
async def setup_endpoint(req: SetupRequest):
    try:
        args = ArgsWrapper(**req.dict())
        setup_environment(args)
        return {"success": True, "message": "Setup complete"}
    except Exception as e:
        logging.exception("Error during setup")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/manage")
async def manage_endpoint(req: ManageRequest):
    try:
        args = ArgsWrapper(**req.dict())
        manage_environment(args)
        return {"success": True, "message": "Manage operations complete"}
    except Exception as e:
        logging.exception("Error during manage")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/teardown")
async def teardown_endpoint(req: TeardownRequest):
    try:
        args = ArgsWrapper(**req.dict())
        teardown_environment(args)
        return {"success": True, "message": "Teardown complete"}
    except Exception as e:
        logging.exception("Error during teardown")
        raise HTTPException(status_code=500, detail=str(e))
