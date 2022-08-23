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
"""Bearer auth for Nessie client."""
from requests import models
from requests.auth import AuthBase


class TokenAuth(AuthBase):
    """AuthBase override for bearer token based auth."""

    def __init__(self, token: str) -> None:
        """Create an AuthBase for the specified bearer auth token."""
        self._token = token

    def __call__(self, r: models.PreparedRequest) -> models.PreparedRequest:
        """Append Auth Header to request."""
        r.headers["Authorization"] = "Bearer {}".format(self._token)
        return r
