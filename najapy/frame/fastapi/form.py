# -*- coding: utf-8 -*-
from typing import List, Any

from fastapi import UploadFile
from najapy.common.async_base import Utils
from pydantic.fields import ModelField
from starlette.datastructures import UploadFile as StarletteUploadFile


class _BaseForm:

    @classmethod
    def __get_validators__(cls):
        yield cls.validate

    @classmethod
    def validate(cls, v: Any, field: ModelField) -> Any:
        raise NotImplementedError

    @classmethod
    def validate_value_type(cls, v, ele_type):
        if not v:
            return v

        if not isinstance(v, ele_type):
            raise ValueError(f'Invalid field value:{v} trans type {ele_type=}')

    @classmethod
    def validate_list_value(cls, v, ele_type=str, sep=','):
        """校验列表元素字段值"""
        if not v:
            return []

        try:

            return [ele_type(x.strip()) for x in v.split(sep)]

        except Exception as _:
            raise ValueError(f'Invalid list field value:{v}')

    @classmethod
    def validate_json_value(cls, v):
        """校验json字段值"""
        if not v:
            return None

        try:

            return Utils.json_decode(v)

        except Exception as _:
            raise ValueError(f'Invalid json field value:{v}')

    @classmethod
    def validate_length_value(cls, v, length=2):
        """校验字段值长度为限定值"""
        if not v:
            return []

        if len(v) != length:
            raise ValueError(f'Invalid list field value:{v}')

        return v

    def __repr__(self):
        return f"{self.__class__.__name__}({super().__repr__()})"


class StrForm(str, _BaseForm):
    @classmethod
    def validate(cls, v: Any, field: ModelField) -> Any:
        if field.field_info.default and not v:
            raise ValueError(f"value of {field.name} is missing.")

        if v:
            cls.validate_value_type(v, str)

            v = v.strip()
            if not v:
                raise ValueError(f"value of {field.name} is missing.")

        return v


class _BaseListForm(_BaseForm):
    _TYPE = None
    _LENGTH = None

    @classmethod
    def validate(cls, v: str, field: ModelField) -> List:

        validate_res = cls.validate_list_value(v, ele_type=cls._TYPE)

        if scope := field.field_info.extra.get('status_in', []):

            for _temp in validate_res:

                if _temp not in scope:
                    raise ValueError(f'Invalid list field value:{v}')

        if cls._LENGTH:
            cls.validate_length_value(validate_res, length=cls._LENGTH)

        return validate_res


class IntListForm(str, _BaseListForm):
    _TYPE = int


class UploadExcelFile(UploadFile):
    _TYPES = ["xlsx", "xls"]

    @classmethod
    def validate(cls, v: Any) -> Any:
        if not isinstance(v, StarletteUploadFile):
            raise ValueError(f"Expected UploadFile, received: {type(v)}")
        try:
            if v.filename.split(".")[-1] not in cls._TYPES:
                raise ValueError("文件格式不正确")
        except Exception:
            raise ValueError("文件格式不正确")

        return v


class DateFormat(str, _BaseForm):
    @classmethod
    def validate(cls, v: Any, field: ModelField) -> Any:
        try:
            cls.validate_value_type(v, str)
            Utils.time2stamp(v, "%Y-%m-%d")
        except ValueError:
            raise ValueError("时间格式不正确，请使用指定的格式（例如：YYYY-MM-DD）")

        return v


class DateTimeFormat(str, _BaseForm):
    @classmethod
    def validate(cls, v: Any, field: ModelField) -> Any:
        try:
            cls.validate_value_type(v, str)
            Utils.time2stamp(v, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            raise ValueError("时间格式不正确，请使用指定的格式（例如：YYYY-MM-DD %H:%M:%S）")

        return v


class DataHourMinuteFormat(str, _BaseForm):
    @classmethod
    def validate(cls, v: Any, field: ModelField) -> Any:
        try:
            cls.validate_value_type(v, str)
            Utils.time2stamp(v, "%H:%M")
        except ValueError:
            raise ValueError("时间格式不正确，请使用指定的格式（例如：%H:%M）")

        return v


class DateYearMonthFormat(str, _BaseForm):
    @classmethod
    def validate(cls, v: Any, field: ModelField) -> Any:
        try:
            cls.validate_value_type(v, str)
            Utils.time2stamp(v, "%Y-%m")
        except ValueError:
            raise ValueError("时间格式不正确，请使用指定的格式（例如：YYYY-MM）")

        return v
