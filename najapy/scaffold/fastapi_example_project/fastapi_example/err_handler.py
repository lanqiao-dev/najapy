from typing import Union

from fastapi import FastAPI
from fastapi.exceptions import RequestValidationError
from pydantic import ValidationError
from starlette import status
from najapy.common.async_base import Utils
from najapy.frame.fastapi.base import Request

from najapy.scaffold.fastapi_example_project.fastapi_example.utils.response import RespCode, ErrorResponse


def exception_handler(fastapi: FastAPI):
    fastapi.add_exception_handler(RequestValidationError, request_validation_exception_handler)
    fastapi.add_exception_handler(ValidationError, request_validation_exception_handler)


async def request_validation_exception_handler(
    _: Request,
    exc: Union[RequestValidationError, ValidationError]
) -> ErrorResponse:
    Utils.log.debug(f"[request_validation_exception_handler] {exc.errors()=}")

    return ErrorResponse(
        error_code=RespCode.InvalidParams,
        status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        details=exc.errors()
    )
