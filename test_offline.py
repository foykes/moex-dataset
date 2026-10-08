"""Substantive default unittest adapter for the deterministic shared suite."""

import importlib.util
from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parent
if 'mds_offline_tests' not in sys.modules:
    specification = importlib.util.spec_from_file_location('mds_offline_tests', ROOT / 'tools/offline_tests.py')
    module = importlib.util.module_from_spec(specification)
    sys.modules[specification.name] = module
    specification.loader.exec_module(module)
offline = sys.modules['mds_offline_tests']
offline.bootstrap_entry()


class OfflineSharedChecks(unittest.TestCase):
    def test_substantive_shared_assertions(self):
        import pytest
        result = int(pytest.main(['-q', str(ROOT / 'tests/shared')]))
        self.assertEqual(result, 0, 'Shared deterministic assertions failed')


if __name__ == '__main__':
    unittest.main()
