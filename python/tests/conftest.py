# -*- coding: utf-8 -*-
"""Configure pytest."""


def pytest_configure(config):  # noqa
    """Configure pytest."""
    config.addinivalue_line("markers", "doc: mark as end-to-end test.")
