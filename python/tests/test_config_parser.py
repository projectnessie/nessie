# -*- coding: utf-8 -*-
"""Tests for `config_parser.py`."""

import os

from assertpy import assert_that

from pynessie.conf.config_parser import build_config


def test_config_from_env() -> None:
    """Makes sure NESSIE environment variables are resolved automatically."""
    os.environ["NESSIE_TEST1_TEST2_TEST3"] = "test_val"
    config = build_config()
    assert_that(config["test1"]["test2"]["test3"].get()).is_equal_to("test_val")


def test_config_from_args() -> None:
    """Makes sure explicit config args are respected."""
    config = build_config({"abc.def": "test_val"})
    assert_that(config["abc"]["def"].get()).is_equal_to("test_val")


def test_args_take_precedence_over_env() -> None:
    """Makes sure explicit config args take precedence over environment variables."""
    os.environ["NESSIE_TEST1_TEST2_TEST3"] = "env_val"
    config = build_config({"test1.test2.test3": "arg_val"})
    assert_that(config["test1"]["test2"]["test3"].get()).is_equal_to("arg_val")
