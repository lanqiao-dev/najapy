# -*- coding: utf-8 -*-

import os

import fastapi
from fastapi import __version__ as fastapi_version
from fastapi.responses import UJSONResponse

from pynaja.common.async_base import Utils
from pynaja.frame.logging import init_logger, DEFAULT_LOG_FILE_ROTATOR
from pynaja.frame.fastapi.response import ErrorResponse


def create_fastapi(
        log_level=r'info', log_handler=None, log_file_path=None,
        log_file_rotation=DEFAULT_LOG_FILE_ROTATOR, log_file_retention=0xff,
        debug=False,
        routes=None,
        **setting
):
    init_logger(
        log_level.upper(),
        log_handler,
        log_file_path,
        log_file_rotation,
        log_file_retention,
        debug
    )

    environment = Utils.environment()

    Utils.log.info(
        f'fastapi {fastapi_version}\n'
        f'python {environment["python"]}\n'
        f'system {" ".join(environment["system"])}'
    )

    return fastapi.FastAPI(debug=debug, routes=routes, **setting)


class Request(fastapi.Request):

    @property
    def referer(self):
        return self.headers.get(r'Referer')

    @property
    def client_ip(self):
        return self.headers.get(r'X-Read-IP', self.client.host)

    @property
    def x_forwarded_for(self):
        return Utils.split_str(self.headers.get(r'X-Forwarded-For', r''), r',')

    @property
    def content_type(self):
        return self.headers.get(r'Content-Type')

    @property
    def content_length(self):
        result = self.headers.get(r'Content-Length', r'')

        return int(result) if result.isdigit() else 0

    def get_header(self, name, default=None):
        return self.headers.get(name, default)


class APIRoute(fastapi.routing.APIRoute):

    def get_route_handler(self):

        original_route_handler = super().get_route_handler()

        async def custom_route_handler(request: fastapi.Request):

            try:
                return await original_route_handler(
                    Request(request.scope, request.receive)
                )
            except ErrorResponse as error:
                return error

        return custom_route_handler


class APIRouter(fastapi.APIRouter):
    """目录可选末尾的斜杠访问
    """

    def __init__(
            self,
            *,
            prefix=r'',
            default_response_class=UJSONResponse,
            route_class=APIRoute,
            **kwargs
    ):

        super().__init__(
            prefix=prefix,
            default_response_class=default_response_class,
            route_class=route_class,
            **kwargs
        )

    def _get_path_alias(self, path):

        _path = path.rstrip(r'/')

        if not _path:
            return [path]

        _path_split = os.path.splitext(_path)

        if _path_split[1]:
            return [_path]

        return [_path, _path + r'/']

    def api_route(self, path, *args, **kwargs):

        def _decorator(func):

            for index, _path in enumerate(self._get_path_alias(path)):

                self.add_api_route(_path, func, *args, **kwargs)

                # 兼容的URL将不会出现在docs中
                if index == 0:
                    kwargs[r'include_in_schema'] = False

            return func

        return _decorator
