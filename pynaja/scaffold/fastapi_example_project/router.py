from pynaja.frame.fastapi.base import APIRouter
from pynaja.scaffold.fastapi_example_project.abc_base.view.base_view import home_router

api_router = APIRouter()

APPLICATION_PREFIX = '/wechat_be'
EXTERNAL_PREFIX = '/external'
INTERNAL_PREFIX = '/internal'
CRONTAB_PREFIX = '/crontab'

internal_prefix = f"{APPLICATION_PREFIX}{INTERNAL_PREFIX}"
external_prefix = f"{APPLICATION_PREFIX}{EXTERNAL_PREFIX}"

##################################################
# 根路径
api_router.include_router(home_router, prefix=f'{APPLICATION_PREFIX}', tags=['home'])

##################################################
# crontab路径


##################################################
# external路径

##################################################
# internal路径
