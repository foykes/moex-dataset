"""Default/explicit F3 collection boundary, installed before project imports."""

import importlib.util
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parent
if 'mds_offline_tests' not in sys.modules:
    specification = importlib.util.spec_from_file_location('mds_offline_tests', ROOT / 'tools/offline_tests.py')
    module = importlib.util.module_from_spec(specification)
    sys.modules[specification.name] = module
    specification.loader.exec_module(module)
offline = sys.modules['mds_offline_tests']
offline.bootstrap_entry()

import pytest


@pytest.hookimpl(tryfirst=True)
def pytest_configure(config):
    offline.configure_shared(config)
