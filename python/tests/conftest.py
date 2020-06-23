# -*- coding: utf-8 -*-
"""Configure pytest."""


def pytest_configure(config):  # noqa
    """Configure pytest."""
    config.addinivalue_line("markers", "e2e: mark as end-to-end test.")
