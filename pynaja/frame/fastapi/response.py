from pynaja.common.struct import Const, Result
from starlette.responses import UJSONResponse


class RespCode():
    Success = 0  # 成功
    Unknown = -1  # 未知


RespMessage = Const()
RespMessage[RespCode.Success] = r"成功"
RespMessage[RespCode.Unknown] = r"未知"


class Response(UJSONResponse):

    def render(self, content):
        return super().render(Result(code=RespCode.Success, data=content, msg=RespMessage[RespCode.Success]))


class ErrorResponse(Exception, UJSONResponse):

    def __init__(self, error_code, content=None, msg=None, status_code=200, details=None, **kwargs):
        self._error_code = error_code
        self._details = details
        self._msg = msg

        Exception.__init__(self)
        UJSONResponse.__init__(self, content, status_code, **kwargs)

    def render(self, content):
        if not self._msg:
            self._msg = RespMessage[self._error_code]

        return super().render(
            Result(code=self._error_code, data=content, msg=self._msg, details=self._details)
        )
