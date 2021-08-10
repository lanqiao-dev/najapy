from najapy.frame.fastapi.base import APIRouter
from najapy.scaffold.fastapi_example_project.fastapi_example.abc_base.view import home_router

api_router = APIRouter()

EXTERNAL_PREFIX = '/external'
INTERNAL_PREFIX = '/internal'
CRONTAB_PREFIX = '/crontab'

##################################################
# 根路径
api_router.include_router(home_router, tags=['home'])

##################################################
# crontab路径


##################################################
# external路径

##################################################
# internal路径
