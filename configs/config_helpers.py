from typing import Iterable

from configs.errors import NullArgumentError, InvalidDateFormat

class ConfigHelpers:

    @staticmethod
    def set_default_value(passed_value, default_value):
        if passed_value is None:
            return default_value
        return passed_value

    @staticmethod
    def check_time_delta_keys(passed_keys, field_name):
        allowed_keys = {"days", "seconds", "microseconds", "milliseconds", "minutes", "hours", "weeks"}
        for passed_key in passed_keys:
            assert passed_key in allowed_keys, f"Allowed keys in {field_name} are {allowed_keys}"

    @staticmethod
    def null_field_check(field_name, field_value):
        if field_value is None:
            raise NullArgumentError(f"Null value found for a non-null attribute: {field_name}")

    @staticmethod
    def parse_date(date_str, field_name):
        try:
            if date_str:
                return eval(date_str)
        except Exception as err:
            raise InvalidDateFormat(f"Please check and fix the {field_name}!")
        return None

    @staticmethod
    def boolify(value):
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.lower() != "false"
        return value

    @staticmethod
    def flatten(l):
        for el in l:
            if isinstance(el, Iterable) and not isinstance(el, (str, bytes)):
                yield from ConfigHelpers.flatten(el)
            else:
                yield el
