"""Shared suite bootstrap. Native --confcutdir lanes keep their own guards."""

import sys
import pytest


@pytest.hookimpl(tryfirst=True)
def pytest_configure(config):
    # Root conftest owns registration; no hidden native configuration rewrite.
    sys.modules['mds_offline_tests'].configure_shared(config)
