from fastapi import APIRouter
from starlette.requests import Request

from rearq.server import templates

router = APIRouter()


@router.get("/")
async def index(request: Request):
    return templates.TemplateResponse(
        "dashboard.html", {"request": request, "page_title": "dashboard"}
    )
