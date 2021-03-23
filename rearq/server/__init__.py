import os

from starlette.templating import Jinja2Templates

from rearq import constants

templates = Jinja2Templates(
    directory=os.path.join(constants.BASE_DIR, "rearq", "server", "templates")
)
