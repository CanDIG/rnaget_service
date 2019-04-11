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
