"""pytest configuration and fixtures for this project.

This file ensures the src directory is in the Python path,
allowing imports of the this package to work correctly.
"""

import sys
from pathlib import Path

# Add src to Python path so tests can import this package
project_root = Path(__file__).parent.parent
src_path = project_root / "src"
sys.path.insert(0, str(src_path))

pytest_plugins = [
    "rd_test_utils.fixtures.config",
    "rd_test_utils.fixtures.lvl1",
    "rd_test_utils.fixtures.lvl2",
]
