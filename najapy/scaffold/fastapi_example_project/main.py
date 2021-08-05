from fastapi import FastAPI
from starlette.middleware.cors import CORSMiddleware
from najapy.frame.fastapi.base import create_fastapi

from najapy.scaffold.fastapi_example_project.activate import ActivateInit
from najapy.scaffold.fastapi_example_project.conf import ConfigDynamic
from najapy.scaffold.fastapi_example_project.err_handler import exception_handler
from najapy.scaffold.fastapi_example_project.router import api_router


def get_application() -> FastAPI:
    fastapi = create_fastapi(
        log_level=ConfigDynamic.LogLevel,
        debug=ConfigDynamic.Debug,
        on_startup=[ActivateInit.activate_initialize],
        on_shutdown=[ActivateInit.activate_release],
        openapi_url=r'/openapi.json',
        docs_url=r'/docs',
        title=r'fastapi_example_project',
    )

    if ConfigDynamic.AllowedHosts:
        fastapi.add_middleware(
            CORSMiddleware,
            allow_origins=ConfigDynamic.AbnormalStatus,
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )

    fastapi.include_router(api_router, prefix='/api/v1')

    exception_handler(fastapi)

    return fastapi


app = get_application()
