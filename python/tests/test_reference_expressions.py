# Copyright (C) 2020 Dremio
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""Tests for `config_parser.py`."""

from assertpy import assert_that

from pynessie.model import (
    DETACHED_REFERENCE_NAME,
    is_valid_hash,
    is_valid_reference_name,
    split_into_reference_and_hash,
)


def test_is_valid_reference_name() -> None:
    """Validates is_valid_reference_name."""
    assert_that(is_valid_reference_name("")).is_false()
    assert_that(is_valid_reference_name("*")).is_false()
    assert_that(is_valid_reference_name("@")).is_false()
    assert_that(is_valid_reference_name("main")).is_true()
    assert_that(is_valid_reference_name("abc/def")).is_true()


def test_is_valid_hash() -> None:
    """Validates is_valid_hash."""
    assert_that(is_valid_hash(".cafe")).is_false()
    assert_that(is_valid_hash("main")).is_false()
    assert_that(is_valid_hash("1122334455667788990011223344556677889900112233445566778899001122")).is_true()
    assert_that(is_valid_hash("abcDEF4242424242424242424242BEEF00DEAD42112233445566778899001122")).is_true()
    assert_that(is_valid_hash("caffee20")).is_true()
    assert_that(is_valid_hash("20caffee")).is_true()


def test_split_into_reference_and_hash() -> None:
    """Validates split_into_reference_and_hash."""
    assert_that(split_into_reference_and_hash("main")).is_equal_to(("main", None))
    assert_that(split_into_reference_and_hash("abcDEF4242424242424242424242BEEF00DEAD42112233445566778899001122")).is_equal_to(
        (DETACHED_REFERENCE_NAME, "abcDEF4242424242424242424242BEEF00DEAD42112233445566778899001122")
    )
    assert_that(split_into_reference_and_hash("main@caffee20")).is_equal_to(("main", "caffee20"))
    assert_that(split_into_reference_and_hash("main*caffee20")).is_equal_to(("main", "caffee20"))
    assert_that(split_into_reference_and_hash("DETACHED@1596af9580555a9a8437553878546385b070440acc063ac4461770ebce763aa1")).is_equal_to(
        (DETACHED_REFERENCE_NAME, "1596af9580555a9a8437553878546385b070440acc063ac4461770ebce763aa1")
    )
    assert_that(split_into_reference_and_hash("1596af9580555a9a8437553878546385b070440acc063ac4461770ebce763aa1")).is_equal_to(
        (DETACHED_REFERENCE_NAME, "1596af9580555a9a8437553878546385b070440acc063ac4461770ebce763aa1")
    )
    assert_that(split_into_reference_and_hash(None)).is_equal_to(("<UNKNOWN>", None))
