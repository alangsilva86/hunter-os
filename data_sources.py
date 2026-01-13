"""Deprecated shim: use modules.data_sources instead."""

import warnings

from modules.data_sources import *  # noqa: F401,F403

warnings.warn(
    "data_sources.py is deprecated; import from modules.data_sources instead.",
    DeprecationWarning,
    stacklevel=2,
)
