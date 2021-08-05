from pynaja.common.struct import Const, Result
from starlette.responses import UJSONResponse

from pynaja.frame.fastapi.response import ErrorResponse as _ErrorResponse


class RespCode():
    Success = 0  # 成功
    Unknown = -1  # 未知
    Database = -2  # 数据库异常
    LackParams = -3  # 缺少必要请求参数
    DataUnExcept = -4  # 异常数据，暂无法操作
    NetError = -5  # 网络错误
    TokenInvalid = -6  # token 无效
    NoPermission = -7  # 无权操作
    InvalidParams = -8  # 无效的参数
    NotSupport = -9  # 不支持的业务类型


RespMessage = Const()
RespMessage[RespCode.Success] = r"成功"
RespMessage[RespCode.Unknown] = r"未知"
RespMessage[RespCode.Database] = r"数据库异常"
RespMessage[RespCode.LackParams] = r"缺少必要请求参数"
RespMessage[RespCode.DataUnExcept] = r'异常数据，暂无法操作'
RespMessage[RespCode.NetError] = r'网络错误，暂无法操作'
RespMessage[RespCode.TokenInvalid] = r"token 无效"
RespMessage[RespCode.NoPermission] = r"无权操作"
RespMessage[RespCode.InvalidParams] = r'无效的参数'
RespMessage[RespCode.NotSupport] = r'不支持的业务类型'


class Response(UJSONResponse):

    def render(self, content):
        return super().render(Result(code=RespCode.Success, data=content, msg=RespMessage[RespCode.Success]))


class ErrorResponse(_ErrorResponse):

    def __init__(self, error_code, content=None, status_code=200, details=None, **kwargs):
        super().__init__(
            error_code=error_code, content=content, msg=RespMessage[error_code], status_code=status_code,
            details=details, **kwargs
        )
