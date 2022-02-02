import datetime
from dataclasses import dataclass
from typing import List, Tuple, Dict, Mapping, Optional, TypeVar

import jsons
from jsons._load_impl import load
from jsons.deserializers.default_primitive import default_primitive_deserializer
from jsons.exceptions import DeserializationError

from configs.config_helpers import ConfigHelpers


T = TypeVar('T')


def custom_string_deserializer(
    obj: object,
    cls: Optional[type] = None,
    **kwargs
) -> object:
    """
    Deserialize a string. If the given ``obj`` can be parsed to a date, a
    ``datetime`` instance is returned.
    :param obj: the string that is to be deserialized.
    :param cls: not used.
    :param kwargs: any keyword arguments.
    :return: the deserialized obj.
    """
    result = None
    if not isinstance(obj, (list, dict, List, Tuple, Dict, Mapping)):
        try:
            result = load(obj, datetime, **kwargs)
        except DeserializationError:
            result = default_primitive_deserializer(obj, str)
    elif isinstance(obj, (list, List, Tuple,)):
        result = jsons.default_list_deserializer(obj, cls)
    elif isinstance(obj, (dict, Dict, Mapping)):
        result = jsons.default_dict_deserializer(obj, dict)
    return result


@dataclass
class Config(jsons.JsonSerializable.set_deserializer(custom_string_deserializer, str)):
    __default_fields__ = []

    __mandatory_fields__ = []

    def __init__(self, config_clazz):
        self.config_clazz = config_clazz

    def __post_init__(self):
        # Default fields check
        for field in self.__default_fields__:
            self.__setattr__(
                field,
                ConfigHelpers.set_default_value(self.__getattribute__(field), self.config_clazz.__getattribute__(field))
            )

        # Mandatory fields check
        for field in self.__mandatory_fields__:
            ConfigHelpers.null_field_check(field, self.__getattribute__(field))


