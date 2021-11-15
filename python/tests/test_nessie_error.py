# -*- coding: utf-8 -*-
"""Authentication tests for Nessi CLI."""
#  Copyright (C) 2020 Dremio
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

from pynessie.error import _create_exception
from pynessie.error import NessieConflictException
from pynessie.error import NessieContentNotFoundException
from pynessie.error import NessieException
from pynessie.error import NessieNotFoundException
from pynessie.error import NessiePermissionException
from pynessie.error import NessiePreconidtionFailedException
from pynessie.error import NessieReferenceAlreadyExistsException
from pynessie.error import NessieReferenceConflictException
from pynessie.error import NessieReferenceNotFoundException
from pynessie.error import NessieServerException
from pynessie.error import NessieUnauthorizedException


# Non-VCR tests for error response handling


def test_raise_exception_missing_payload() -> None:
    """Test the handling error responses with missing JSON payload."""
    ex = _create_exception({}, 412, "reason123", "url123")
    assert isinstance(ex, NessiePreconidtionFailedException)
    assert "412" in str(ex.json())
    assert "reason123" in str(ex.json())
    assert "url123" in str(ex.json())

    ex = _create_exception({}, 401, "reason123", "url123")
    assert isinstance(ex, NessieUnauthorizedException)

    ex = _create_exception({}, 403, "reason123", "url123")
    assert isinstance(ex, NessiePermissionException)

    ex = _create_exception({}, 404, "reason123", "url123")
    assert isinstance(ex, NessieNotFoundException)

    ex = _create_exception({}, 409, "reason123", "url123")
    assert isinstance(ex, NessieConflictException)

    ex = _create_exception({}, 599, "reason123", "url123")
    assert isinstance(ex, NessieServerException)
    assert "599" in str(ex.json())
    assert "Server Error" in str(ex)
    assert "Server Error" in str(ex.json())
    assert "Internal Server Error" in str(ex.json())

    ex = _create_exception({}, 12345, "reason123", "url123")
    assert isinstance(ex, NessieException)
    assert "12345" in str(ex.json())


def _test_error_code(error_code: str, exception: type) -> None:
    ex = _create_exception(dict(message="msg123", status=499, errorCode=error_code), 498, "reason123", "url123")
    assert isinstance(ex, exception)
    assert "Client Error" in str(ex)
    assert "msg123" in str(ex)
    assert "reason123" in str(ex)
    assert "Client Error" in str(ex.json())
    assert "499" in str(ex.json())
    assert "498" in str(ex.json())
    assert "msg123" in str(ex.json())
    assert "reason123" in str(ex.json())
    assert "url123" in str(ex.json())


def test_raise_ref_not_found() -> None:
    """Test the handling error code REFERENCE_NOT_FOUND."""
    _test_error_code("REFERENCE_NOT_FOUND", NessieReferenceNotFoundException)


def test_raise_content_not_found() -> None:
    """Test the handling error code CONTENT_NOT_FOUND."""
    _test_error_code("CONTENT_NOT_FOUND", NessieContentNotFoundException)


def test_raise_ref_conflict() -> None:
    """Test the handling error code REFERENCE_CONFLICT."""
    _test_error_code("REFERENCE_CONFLICT", NessieReferenceConflictException)


def test_raise_ref_already_exists() -> None:
    """Test the handling error code REFERENCE_ALREADY_EXISTS."""
    _test_error_code("REFERENCE_ALREADY_EXISTS", NessieReferenceAlreadyExistsException)
