from fastapi import APIRouter
from starlette.requests import Request
from starlette.templating import Jinja2Templates

router = APIRouter()
templates = Jinja2Templates(directory="rearq/server/templates")


@router.get("/")
async def index(request: Request):
    return templates.TemplateResponse(
        "dashboard.html", {"request": request, "page_title": "dashboard"}
    )


@router.get("/workers")
async def index(request: Request):
    return templates.TemplateResponse("workers.html", {"request": request, "page_title": "workers"})


@router.get("/tasks")
async def index(request: Request):
    return templates.TemplateResponse("tasks.html", {"request": request, "page_title": "tasks"})


@router.get("/results")
async def index(request: Request):
    return templates.TemplateResponse("results.html", {"request": request, "page_title": "results"})
