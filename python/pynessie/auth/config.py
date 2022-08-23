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
"""Config based auth mechanism."""

from typing import Optional

from confuse import Configuration
from requests.auth import AuthBase, HTTPBasicAuth

from pynessie.auth.aws import setup_aws_auth
from pynessie.auth.bearer import TokenAuth as BearerTokenAuth


def setup_auth(config: Configuration) -> Optional[AuthBase]:
    """Setup an AuthBase object for future authentication against a Nessie Server.

    :param config: confuse.Configuration object
    :return: a requests.auth.AuthBase object that is able to inject authentication data into HTTP requests
    """
    auth_type = config["auth"]["type"].get()

    if auth_type == "none":
        return None
    if auth_type == "basic":
        username = config["auth"]["username"].get()
        password = config["auth"]["password"].get()
        return HTTPBasicAuth(username, password)
    if auth_type == "bearer":
        token = config["auth"]["token"].get()
        return BearerTokenAuth(token)
    if auth_type == "aws":
        region = config["auth"]["region"].get()
        profile = config["auth"]["profile"].get()
        return setup_aws_auth(region=region, profile=profile)

    raise NotImplementedError("Unsupported authentication type: " + auth_type)
