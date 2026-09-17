"""Root proxy for utilities to support `%run ./utilities` from workspace root or subdirectories.
Delegates directly to 1_setup/utilities.py.
"""
import os
import sys

_setup_utilities = os.path.join(os.path.dirname(os.path.abspath(__file__)), "1_setup", "utilities.py")
if os.path.exists(_setup_utilities):
    with open(_setup_utilities, "r", encoding="utf-8") as _f:
        _code = _f.read()
    exec(compile(_code, _setup_utilities, "exec"), globals())
