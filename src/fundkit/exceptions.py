"""Exception Classes."""


class CacheCreationError(Exception):
    """Error occured in NAV cache creation."""

    pass


class PandasExportError(Exception):
    """Error occured in converting to pandas DataFrame."""

    pass


class InvalidAMFIResponseError(Exception):
    """Error occured while parsing invalid AMFI response."""

    pass
