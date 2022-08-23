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
"""Test Nessie models."""
from assertpy import assert_that

from pynessie.model import ContentKey


def test_content_key_to_string() -> None:
    """Test ContentKey.to_string method."""
    assert_that(ContentKey(["a", "b", "c"]).to_string()).is_equal_to("a.b.c")
    assert_that(ContentKey(["a", "b", "c.d"]).to_string()).is_equal_to('a.b."c.d"')
    assert_that(ContentKey(["a.b", "c", "d"]).to_string()).is_equal_to('"a.b".c.d')
    assert_that(ContentKey(["a.b", "c", "d.e"]).to_string()).is_equal_to('"a.b".c."d.e"')
    assert_that(ContentKey(["a", "b.c", "d"]).to_string()).is_equal_to('a."b.c".d')


def test_content_key_from_path_string() -> None:
    """Test ContentKey.from_path_string method."""
    assert_that(ContentKey.from_path_string("a.b.c")).is_equal_to(ContentKey(["a", "b", "c"]))
    assert_that(ContentKey.from_path_string("a.b.c\00d")).is_equal_to(ContentKey(["a", "b", "c.d"]))
    assert_that(ContentKey.from_path_string("a\00b.c.d")).is_equal_to(ContentKey(["a.b", "c", "d"]))
    assert_that(ContentKey.from_path_string("a\00b.c.d\00e")).is_equal_to(ContentKey(["a.b", "c", "d.e"]))
    assert_that(ContentKey.from_path_string("a.b\00c.d")).is_equal_to(ContentKey(["a", "b.c", "d"]))


def test_content_key_to_path_string() -> None:
    """Test ContentKey.to_path_string method."""
    assert_that(ContentKey(["a", "b", "c"]).to_path_string()).is_equal_to("a.b.c")
    assert_that(ContentKey(["a", "b", "c.d"]).to_path_string()).is_equal_to("a.b.c\00d")
    assert_that(ContentKey(["a.b", "c", "d"]).to_path_string()).is_equal_to("a\00b.c.d")
    assert_that(ContentKey(["a.b", "c", "d.e"]).to_path_string()).is_equal_to("a\00b.c.d\00e")
    assert_that(ContentKey(["a", "b.c", "d"]).to_path_string()).is_equal_to("a.b\00c.d")
