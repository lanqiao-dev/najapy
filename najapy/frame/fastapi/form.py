# -*- coding: utf-8 -*-
from typing import List, Any, Dict, Union

from najapy.common.async_base import Utils
from pydantic.fields import ModelField


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

        try:
            v = ele_type(v)
        except Exception as _:
            raise ValueError(f'Invalid field value:{v} trans type {ele_type=}')

        return v

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


class _BaseEnumForm(_BaseForm):
    _TYPE = None

    @classmethod
    def validate(cls, v: Any, field: ModelField) -> Union[str, int]:
        validate_v = cls.validate_value_type(v, cls._TYPE)

        if scope := field.field_info.extra.get('status_in', []):

            if validate_v not in scope:
                raise ValueError(f'Invalid field value:{v}')

        return validate_v


class _BaseListForm(_BaseForm):
    _TYPE = None
    _LENGTH = None

    @classmethod
    def validate(cls, v: str, field: ModelField) -> List:

        validate_v = cls.validate_list_value(v, ele_type=cls._TYPE)

        if scope := field.field_info.extra.get('status_in', []):

            for _temp in validate_v:

                if _temp not in scope:
                    raise ValueError(f'Invalid list field value:{v}')

        if cls._LENGTH:
            cls.validate_length_value(validate_v, length=cls._LENGTH)

        return validate_v


class IntEnumForm(int, _BaseEnumForm):
    _TYPE = int


class StrEnumForm(str, _BaseEnumForm):
    _TYPE = str


class IntListForm(str, _BaseListForm):
    _TYPE = int


class IntListLength2Form(IntListForm):
    _LENGTH = 2


class StrListForm(str, _BaseForm):
    _TYPE = str


class StrListLength2Form(StrListForm):
    _LENGTH = 2


class JsonForm(str, _BaseForm):
    @classmethod
    def validate(cls, v: str, field: ModelField) -> Dict:
        validate_res = cls.validate_json_value(v)

        return validate_res
