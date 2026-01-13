"""Deprecated legacy module. Use modules/* equivalents instead."""

import warnings

warnings.warn(
    "utils.py is deprecated; use modules/* or update your imports.",
    DeprecationWarning,
    stacklevel=2,
)

__all__ = []
