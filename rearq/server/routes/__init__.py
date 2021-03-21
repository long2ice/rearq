from fastapi import APIRouter

from .job import router as job_router
from .task import router as task_router
from .views import router as views_router
from .worker import router as worker_router

view_router = APIRouter()
api_router = APIRouter()

view_router.include_router(views_router, tags=["Views"])

api_router.include_router(job_router, prefix="/job", tags=["Job"])
api_router.include_router(task_router, prefix="/task", tags=["Task"])
api_router.include_router(worker_router, prefix="/worker", tags=["Worker"])
