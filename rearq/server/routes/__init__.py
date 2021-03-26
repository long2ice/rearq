from fastapi import APIRouter

from .index import router as index_router
from .job import router as job_router
from .result import router as result_router
from .task import router as task_router
from .worker import router as worker_router

router = APIRouter()

router.include_router(index_router, tags=["Index"])
router.include_router(job_router, prefix="/job", tags=["Job"])
router.include_router(task_router, prefix="/task", tags=["Task"])
router.include_router(worker_router, prefix="/worker", tags=["Worker"])
router.include_router(result_router, prefix="/result", tags=["Result"])
