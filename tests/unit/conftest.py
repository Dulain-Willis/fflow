# tests/unit/conftest.py
#
# Stub out native C-extension libraries before any test module is imported.
# Lets unit tests run without ODBC driver or JVM installed.

import sys
from unittest.mock import MagicMock

sys.modules.setdefault("pyodbc", MagicMock())
sys.modules.setdefault("jaydebeapi", MagicMock())
sys.modules.setdefault("jpype", MagicMock())
sys.modules.setdefault("jpype.types", MagicMock())
