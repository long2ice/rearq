import uvicorn
from fastapi import FastAPI
from tortoise.contrib.fastapi import register_tortoise

from examples import settings
from examples.models import Test
from examples.tasks import rearq, add

app = FastAPI()

register_tortoise(
    app,
    db_url=settings.DB_URL,
    modules={
        "models": ["examples.models"],
    },
    generate_schemas=True,
    add_exception_handlers=True,
)


@app.on_event("startup")
async def startup():
    await rearq.init()


@app.on_event("shutdown")
async def shutdown():
    await rearq.close()


@app.get("/")
async def index():
    print(await Test.all())
    await add.delay(1, 1, countdown=1)
    job = await add.delay(a=2, b=2, countdown=1)
    result = await job.result(timeout=5)
    return {"result": result}


if __name__ == "__main__":
    uvicorn.run("examples.app:app", reload=True)
