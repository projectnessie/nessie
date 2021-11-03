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

"""Utility functions that can be used to manipulate contents like keys and values."""

import re

from ..model import ContentsKey

_dot_regex = re.compile('\\.(?=([^"]*"[^"]*")*[^"]*$)')


def format_key(raw_key: str) -> str:
    """Format given key based on regex."""
    elements = _dot_regex.split(raw_key)
    return ".".join(i.replace(".", "\0") for i in elements if i)


def contents_key(raw_key: str) -> ContentsKey:
    """Format given content key based on regex."""
    elements = _dot_regex.split(raw_key)
    return ContentsKey([i for i in elements if i])
