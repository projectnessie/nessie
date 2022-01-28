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

"""Direct API operations on Nessie with requests."""

from typing import Any
from typing import cast
from typing import Optional
from typing import Union

import requests
import simplejson as jsonlib
from requests.auth import AuthBase

from ..error import _create_exception
from ..model import ContentKey


def _get_headers(has_body: bool = False) -> dict:
    headers = {"Accept": "application/json"}
    if has_body:
        headers = {"Content-Type": "application/json"}
    return headers


def _get(url: str, auth: Optional[AuthBase], ssl_verify: bool = True, params: dict = None) -> Union[str, dict, list]:
    r = requests.get(url, headers=_get_headers(), verify=ssl_verify, params=params, auth=auth)
    return _check_error(r)


def _post(
    url: str, auth: Optional[AuthBase], json: Union[str, dict] = None, ssl_verify: bool = True, params: dict = None
) -> Union[str, dict, list]:
    if isinstance(json, str):
        json = jsonlib.loads(json)
    r = requests.post(url, headers=_get_headers(json is not None), verify=ssl_verify, json=json, params=params, auth=auth)
    return _check_error(r)


def _delete(url: str, auth: Optional[AuthBase], ssl_verify: bool = True, params: dict = None) -> Union[str, dict, list]:
    r = requests.delete(url, headers=_get_headers(), verify=ssl_verify, params=params, auth=auth)
    return _check_error(r)


def _put(url: str, auth: Optional[AuthBase], json: Union[str, dict] = None, ssl_verify: bool = True, params: dict = None) -> Any:
    if isinstance(json, str):
        json = jsonlib.loads(json)
    r = requests.put(url, headers=_get_headers(json is not None), verify=ssl_verify, json=json, params=params, auth=auth)
    return _check_error(r)


def _check_error(r: requests.models.Response) -> Union[dict, list]:
    if 200 <= r.status_code < 300:
        return r.json() if r.content else {}

    if isinstance(r.reason, bytes):
        try:
            reason = r.reason.decode("utf-8")
        except UnicodeDecodeError:
            reason = r.reason.decode("iso-8859-1")
    else:
        reason = r.reason

    try:
        parsed_response = r.json()
    except:  # NOQA # pylint: disable=W0702
        # rare/unexpected case when the server responds with a non-JSON payload for an error
        parsed_response = {}

    raise _create_exception(parsed_response, r.status_code, reason, r.url)


def all_references(base_url: str, auth: Optional[AuthBase], ssl_verify: bool = True, fetch_all: bool = False) -> dict:
    """Fetch all known references.

    :param base_url: base Nessie url
    :param auth: Authentication settings
    :param ssl_verify: ignore ssl errors if False
    :param fetch_all: indicates whether additional metadata should be fetched
    :return: json list of Nessie references
    """
    params = {}
    if fetch_all:
        params["fetch"] = "ALL"
    return cast(dict, _get(base_url + "/trees", auth, ssl_verify=ssl_verify, params=params))


def get_reference(base_url: str, auth: Optional[AuthBase], ref: str, ssl_verify: bool = True) -> dict:
    """Fetch a reference.

    :param base_url: base Nessie url
    :param auth: Authentication settings
    :param ref: name of ref to fetch
    :param ssl_verify: ignore ssl errors if False
    :return: json Nessie branch or tag
    """
    return cast(dict, _get(base_url + "/trees/tree/{}".format(ref), auth, ssl_verify=ssl_verify))


def create_reference(base_url: str, auth: Optional[AuthBase], ref_json: dict, source_ref: str = None, ssl_verify: bool = True) -> dict:
    """Create a reference.

    :param base_url: base Nessie url
    :param auth: Authentication settings
    :param ref_json: reference to create as json object
    :param source_ref: name of the reference via which the hash in 'ref_json' is reachable
    :param ssl_verify: ignore ssl errors if False
    :return: json Nessie branch or tag
    """
    params = {}
    if source_ref:
        params["sourceRefName"] = source_ref
    return cast(dict, _post(base_url + "/trees/tree", auth, ref_json, ssl_verify=ssl_verify, params=params))


def get_default_branch(base_url: str, auth: Optional[AuthBase], ssl_verify: bool = True) -> dict:
    """Fetch a reference.

    :param base_url: base Nessie url
    :param auth: Authentication settings
    :param ssl_verify: ignore ssl errors if False
    :return: json Nessie branch
    """
    return cast(dict, _get(base_url + "/trees/tree", auth, ssl_verify=ssl_verify))


def delete_branch(base_url: str, auth: Optional[AuthBase], branch: str, hash_: str, ssl_verify: bool = True) -> None:
    """Delete a branch.

    :param base_url: base Nessie url
    :param auth: Authentication settings
    :param branch: name of branch to delete
    :param hash_: branch hash
    :param ssl_verify: ignore ssl errors if False
    """
    params = {"expectedHash": hash_}
    _delete(base_url + "/trees/branch/{}".format(branch), auth, ssl_verify=ssl_verify, params=params)


def delete_tag(base_url: str, auth: Optional[AuthBase], tag: str, hash_: str, ssl_verify: bool = True) -> None:
    """Delete a tag.

    :param base_url: base Nessie url
    :param auth: Authentication settings
    :param tag: name of tag to delete
    :param hash_: tag hash
    :param ssl_verify: ignore ssl errors if False
    """
    params = {"expectedHash": hash_}
    _delete(base_url + "/trees/tag/{}".format(tag), auth, ssl_verify=ssl_verify, params=params)


def list_tables(
    base_url: str,
    auth: Optional[AuthBase],
    ref: str,
    hash_on_ref: Optional[str] = None,
    max_result_hint: Optional[int] = None,
    page_token: Optional[str] = None,
    query_filter: Optional[str] = None,
    ssl_verify: bool = True,
) -> list:
    """Fetch a list of all tables from a known reference.

    :param base_url: base Nessie url
    :param auth: Authentication settings
    :param ref: reference
    :param hash_on_ref: hash on reference
    :param max_result_hint: hint for the server, maximum number of results to return
    :param page_token: the token retrieved from a previous page returned for the same ref
    :param query_filter: A CEL expression that allows advanced filtering capabilities
    :param ssl_verify: ignore ssl errors if False
    :return: json list of Nessie table names
    """
    params = {}
    if max_result_hint:
        params["maxRecords"] = str(max_result_hint)
    if hash_on_ref:
        params["hashOnRef"] = hash_on_ref
    if page_token:
        params["pageToken"] = page_token
    if query_filter:
        params["filter"] = query_filter
    return cast(list, _get(base_url + "/trees/tree/{}/entries".format(ref), auth, ssl_verify=ssl_verify, params=params))


def list_logs(
    base_url: str,
    auth: Optional[AuthBase],
    ref: str,
    hash_on_ref: Optional[str] = None,
    ssl_verify: bool = True,
    max_records: Optional[int] = None,
    fetch_all: bool = False,
    **filtering_args: Any,
) -> dict:
    """Fetch a list of all logs from a known starting reference.

    :param base_url: base Nessie url
    :param auth: Authentication settings
    :param ref: starting reference
    :param hash_on_ref: hash on reference
    :param max_records: maximum number of entries to return
    :param ssl_verify: ignore ssl errors if False
    :param fetch_all: indicates whether additional metadata should be fetched
    :param filtering_args: All of the args used to filter the log
    :return: json dict of Nessie logs
    """
    params = filtering_args
    if hash_on_ref:
        params["hashOnRef"] = hash_on_ref
    if max_records:
        params["maxRecords"] = max_records
    if fetch_all:
        params["fetch"] = "ALL"
    return cast(dict, _get(base_url + "/trees/tree/{}/log".format(ref), auth, ssl_verify=ssl_verify, params=filtering_args))


def get_content(
    base_url: str, auth: Optional[AuthBase], ref: str, content_key: ContentKey, hash_on_ref: Optional[str] = None, ssl_verify: bool = True
) -> dict:
    """Fetch a table from a known branch.

    :param base_url: base Nessie url
    :param auth: Authentication settings
    :param ref: ref
    :param hash_on_ref: hash on reference
    :param content_key: key that is associated with content like table
    :param ssl_verify: ignore ssl errors if False
    :return: json dict of Nessie table
    """
    params = {"ref": ref}
    if hash_on_ref:
        params["hashOnRef"] = hash_on_ref
    return cast(dict, _get(base_url + "/contents/{}".format(content_key.to_path_string()), auth, ssl_verify=ssl_verify, params=params))


def assign_branch(
    base_url: str, auth: Optional[AuthBase], branch: str, assign_to_json: dict, old_hash: Optional[str], ssl_verify: bool = True
) -> None:
    """Assign a reference to a branch.

    :param base_url: base Nessie url
    :param auth: Authentication settings
    :param branch: name of the branch
    :param assign_to_json: hash to become the new HEAD of the branch and the name of the reference via which that hash is reachable
    :param old_hash: current hash of the branch
    :param ssl_verify: ignore ssl errors if False
    """
    url = "/trees/branch/{}".format(branch)
    params = {"expectedHash": old_hash}
    _put(base_url + url, auth, assign_to_json, ssl_verify=ssl_verify, params=params)


def assign_tag(
    base_url: str, auth: Optional[AuthBase], tag: str, assign_to_json: dict, old_hash: Optional[str], ssl_verify: bool = True
) -> None:
    """Assign a reference to a tag.

    :param base_url: base Nessie url
    :param auth: Authentication settings
    :param tag: name of the tag
    :param assign_to_json: hash to become the new HEAD of the tag and the name of the reference via which that hash is reachable
    :param old_hash: current hash of the tag
    :param ssl_verify: ignore ssl errors if False
    """
    url = "/trees/tag/{}".format(tag)
    params = {"expectedHash": old_hash}
    _put(base_url + url, auth, assign_to_json, ssl_verify=ssl_verify, params=params)


def cherry_pick(
    base_url: str, auth: Optional[AuthBase], branch: str, transplant_json: dict, expected_hash: Optional[str], ssl_verify: bool = True
) -> None:
    """cherry-pick a list of hashes to a branch.

    :param base_url: base Nessie url
    :param auth: Authentication settings
    :param branch: name of branch to cherry pick onto
    :param transplant_json: transplant content
    :param expected_hash: expected hash of HEAD of branch
    :param ssl_verify: ignore ssl errors if False
    """
    url = "/trees/branch/{}/transplant".format(branch)
    params = {}
    if expected_hash:
        params["expectedHash"] = expected_hash
    _post(base_url + url, auth, json=transplant_json, ssl_verify=ssl_verify, params=params)


def merge(
    base_url: str, auth: Optional[AuthBase], branch: str, merge_json: dict, expected_hash: Optional[str], ssl_verify: bool = True
) -> None:
    """Merge a branch into another branch.

    :param base_url: base Nessie url
    :param auth: Authentication settings
    :param branch: name of branch to merge onto
    :param merge_json: merge content
    :param expected_hash: expected hash of HEAD of branch
    :param ssl_verify: ignore ssl errors if False
    """
    url = "/trees/branch/{}/merge".format(branch)
    params = {}
    if expected_hash:
        params["expectedHash"] = expected_hash
    _post(base_url + url, auth, json=merge_json, ssl_verify=ssl_verify, params=params)


def commit(
    base_url: str,
    auth: Optional[AuthBase],
    branch: str,
    operations: str,
    expected_hash: str,
    ssl_verify: bool = True,
) -> dict:
    """Commit a set of operations to a branch.

    :param base_url: base Nessie url
    :param auth: Authentication settings
    :param branch: name of branch to merge onto
    :param operations: json object of operations
    :param expected_hash: expected hash of HEAD of branch
    :param ssl_verify: ignore ssl errors if False
    """
    url = "/trees/branch/{}/commit".format(branch)
    params = {"expectedHash": expected_hash}
    return cast(dict, _post(base_url + url, auth, json=operations, ssl_verify=ssl_verify, params=params))


def get_diff(
    base_url: str,
    auth: Optional[AuthBase],
    from_ref: str,
    to_ref: str,
    from_hash_on_ref: Optional[str] = None,
    to_hash_on_ref: Optional[str] = None,
    ssl_verify: bool = True,
) -> dict:
    """Fetch the diff for two given references.

    :param base_url: base Nessie url
    :param auth: Authentication settings
    :param from_ref: the starting ref for the diff
    :param to_ref:  the ending ref for the diff
    :param from_hash_on_ref: optional hash on from reference
    :param to_hash_on_ref: optional hash on to reference
    :param ssl_verify: ignore ssl errors if False
    :return: json dict of a Diff
    """
    from_hash_on_ref_asterisk = f"*{from_hash_on_ref}" if from_hash_on_ref else ""
    to_hash_on_ref_asterisk = f"*{to_hash_on_ref}" if to_hash_on_ref else ""
    return cast(
        dict,
        _get(f"{base_url}/diffs/{from_ref}{from_hash_on_ref_asterisk}...{to_ref}{to_hash_on_ref_asterisk}", auth, ssl_verify=ssl_verify),
    )


def list_reflog(
    base_url: str,
    auth: Optional[AuthBase],
    ssl_verify: bool = True,
    max_records: Optional[int] = None,
    **query_params: Any,
) -> dict:
    """Fetch a list of all reflog from a head or from a specified range.

    :param base_url: base Nessie url
    :param auth: Authentication settings
    :param max_records: maximum number of entries to return
    :param ssl_verify: ignore ssl errors if False
    :param query_params: All of the args used to filter the log
    :return: json dict of Nessie logs
    """
    params = query_params
    if max_records:
        params["maxRecords"] = max_records
    return cast(dict, _get(f"{base_url}/reflogs", auth, ssl_verify=ssl_verify, params=query_params))
