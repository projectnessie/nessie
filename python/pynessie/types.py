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
"""Custom types that can be used with click."""
from typing import Any, Optional

import click
from click import Context, Parameter

from pynessie.model import ContentKey


class ContentKeyParamType(click.ParamType):
    """ContentType click type."""

    name = "content_key"

    # pylint: disable=R1710
    def convert(self, value: Any, param: Optional[Parameter], ctx: Optional[Context]) -> ContentKey:
        """Convert incoming param into ContentKey type."""
        if isinstance(value, ContentKey):
            return value

        try:
            return ContentKey.from_path_string(str(value))
        except (TypeError, ValueError) as e:
            self.fail(f"{value!r} is not a valid content key due to: '{e!r}'", param, ctx)


CONTENT_KEY = ContentKeyParamType()
