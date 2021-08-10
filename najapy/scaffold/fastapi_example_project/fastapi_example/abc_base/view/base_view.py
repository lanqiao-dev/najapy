from fastapi import APIRouter
from starlette import status

from najapy.scaffold.fastapi_example_project.fastapi_example.abc_base.service.base_service import DataSource
from najapy.scaffold.fastapi_example_project.fastapi_example.utils.response import Response, ErrorResponse, RespCode

home_router = APIRouter()


@home_router.get(r'/')
async def default():
    if DataSource().online:
        return Response(status_code=status.HTTP_200_OK)
    else:
        return ErrorResponse(error_code=RespCode.Unknown, status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)


@home_router.head(r'/health')
async def default():
    ok = await DataSource().health()
    if not ok:
        return ErrorResponse(error_code=RespCode.Unknown, status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)
    else:
        return Response(status_code=status.HTTP_200_OK)
