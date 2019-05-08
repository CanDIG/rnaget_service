# pylint: disable=invalid-name
# pylint: disable=C0301
"""
Custom exceptions for API operations
"""


class ThresholdValueError(ValueError):
    """
    Custom exception for threshold arrays when passed in as query parameters
    """
    def __init__(self):
        message = "Threshold array must be formatted: feature_1,threshold_1,...,feature_n,threshold_n"
        super().__init__(message)


class IdentifierFormatError(ValueError):
    """
    Custom exception for validation fail on UUID string parameters
    """
    def __init__(self, identifier):
        message = "{} parameters must be correctly formatted UUID strings".format(identifier)
        super().__init__(message)


class AuthorizationError(Exception):
    """
    Custom exception for failed authorization
    """
    def __init__(self):
        message = "Key not authorized to perform this action"
        super().__init__(message)
