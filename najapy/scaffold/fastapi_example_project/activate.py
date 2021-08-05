from najapy.scaffold.fastapi_example_project.abc_base.service.base_service import DataSource


class ActivateInit:

    @staticmethod
    async def activate_initialize():
        await DataSource.initialize()

    @staticmethod
    async def activate_release():
        await DataSource().release()
