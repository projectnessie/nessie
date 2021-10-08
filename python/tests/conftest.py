# -*- coding: utf-8 -*-
"""Configure pytest."""

import os
import shutil
import tempfile

import pytest
from pytest import Session


def pytest_configure(config):  # noqa
    """Configure pytest."""
    config.addinivalue_line("markers", "doc: mark as end-to-end test.")


def pytest_sessionstart(session: "Session") -> None:
    """Setup a fresh temporary config directory for tests."""
    pytest.nessie_config_dir = tempfile.mkdtemp() + "/"
    # Instruct Confuse to keep Nessie config file in the temp location:
    os.environ["NESSIEDIR"] = pytest.nessie_config_dir


def pytest_sessionfinish(session: "Session", exitstatus: int) -> None:
    """Remove temporary config directory."""
    shutil.rmtree(pytest.nessie_config_dir)
