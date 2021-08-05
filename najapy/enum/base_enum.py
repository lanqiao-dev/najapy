from najapy.common.metaclass import Singleton


class Enum:

    def __init__(self, code, message, value=0):
        self.code = code
        self.message = message
        self.value = value

    def __eq__(self, other):
        if isinstance(other, Enum):
            return self.code == other.code
        elif self.code == other:
            return True
        else:
            return False


class BaseEnum(Singleton):
    _attr_list = None
    _attr_dict = None
    _attr_dict_enum = None
    _attr_weight_dict = None

    @classmethod
    def to_list(cls):

        if cls._attr_list:
            return cls._attr_list

        attr_list = []

        for attr in dir(cls):

            if not attr.startswith("__"):
                attr_obj = getattr(cls, attr)
                if isinstance(attr_obj, Enum):
                    attr_list.append(attr_obj)
        attr_list = sorted([attr.code for attr in attr_list])

        cls._attr_list = attr_list
        return attr_list

    @classmethod
    def to_dict(cls):

        if cls._attr_dict:
            return cls._attr_dict

        attr_dict = {}

        for attr in dir(cls):

            if not attr.startswith("__"):
                attr_obj = getattr(cls, attr)
                if isinstance(attr_obj, Enum):
                    attr_dict[attr_obj.code] = attr_obj.message

        cls._attr_dict = attr_dict
        return attr_dict

    @classmethod
    def to_dict_enum(cls):

        if cls._attr_dict_enum:
            return cls._attr_dict_enum

        attr_dict_enum = {}

        for attr in dir(cls):

            if not attr.startswith("__"):
                attr_obj = getattr(cls, attr)
                if isinstance(attr_obj, Enum):
                    attr_dict_enum[attr_obj.code] = attr_obj

        cls._attr_dict_enum = attr_dict_enum
        return attr_dict_enum

    @classmethod
    def to_weight(cls):

        if cls._attr_weight_dict:
            return cls._attr_weight_dict

        _attr_weight_dict = {}

        for attr in dir(cls):

            if not attr.startswith("__"):
                attr_obj = getattr(cls, attr)
                if isinstance(attr_obj, Enum):
                    _attr_weight_dict[attr_obj.code] = attr_obj.value

        cls._attr_weight_dict = _attr_weight_dict
        return _attr_weight_dict

    def __iter__(self):
        return iter(self.to_list())

    def __repr__(self):
        return '%s' % self.to_list()
