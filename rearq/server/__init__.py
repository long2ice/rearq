import os
from datetime import date

from starlette.templating import Jinja2Templates

from rearq import constants, version

templates = Jinja2Templates(
    directory=os.path.join(constants.STATIC_DIR, "rearq", "server", "templates")
)
templates.env.globals["REAEQ_VERSION"] = version.VERSION
templates.env.globals["NOW_YEAR"] = date.today().year
