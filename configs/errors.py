class NullArgumentError(Exception):
    pass

class InvalidDateFormat(Exception):
    pass

class DefinedFuncSyntaxError(SyntaxError):
    pass

class DefinedFuncNotFoundError(NameError):
    pass
