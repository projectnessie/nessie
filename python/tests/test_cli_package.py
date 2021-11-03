# -*- coding: utf-8 -*-
#
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

"""Tests for pynessie package setup."""

import os

from assertpy import assert_that


def test_all_valid_modules_to_have_init() -> None:
    """Test if all folder in pynessie have the required '__init__.py' file in order to treat them as modules."""
    pynessie_dir = os.path.join(os.getcwd(), "pynessie")
    _test_if_file_exists_nested_folders(pynessie_dir, "__init__.py")


def _test_if_file_exists_nested_folders(input_folder: str, expected_file_name: str) -> None:
    for file in os.listdir(input_folder):
        file_full_path = os.path.join(input_folder, file)
        if os.path.isdir(file_full_path) and file not in "__pycache__":
            assert_that(os.path.join(file_full_path, expected_file_name)).exists()
            _test_if_file_exists_nested_folders(file_full_path, expected_file_name)
