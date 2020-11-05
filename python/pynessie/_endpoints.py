# -*- coding: utf-8 -*-
"""Direct API operations on Nessie with requests."""
from typing import Any
from typing import cast
from typing import Optional
from typing import Tuple
from typing import Union

import requests
import simplejson as jsonlib
from requests.exceptions import HTTPError

from .error import NessieConflictException
from .error import NessieException
from .error import NessieNotFoundException
from .error import NessiePermissionException
from .error import NessiePreconidtionFailedException
from .error import NessieServerException
from .error import NessieUnauthorizedException


def _get_headers(has_body: bool = False) -> dict:
    headers = {"Accept": "application/json"}
    if has_body:
        headers = {"Content-Type": "application/json"}
    return headers


def _get(url: str, details: str = "", ssl_verify: bool = True, params: dict = None) -> Union[str, dict, list]:
    r = requests.get(url, headers=_get_headers(), verify=ssl_verify, params=params)
    return _check_error(r, details)


def _post(
    url: str, json: Union[str, dict] = None, details: str = "", ssl_verify: bool = True, params: dict = None
) -> Union[str, dict, list]:
    if isinstance(json, str):
        json = jsonlib.loads(json)
    r = requests.post(url, headers=_get_headers(json is not None), verify=ssl_verify, json=json, params=params)
    return _check_error(r, details)


def _delete(url: str, details: str = "", ssl_verify: bool = True, params: dict = None) -> Union[str, dict, list]:
    r = requests.delete(url, headers=_get_headers(), verify=ssl_verify, params=params)
    return _check_error(r, details)


def _put(url: str, json: Union[str, dict] = None, details: str = "", ssl_verify: bool = True, params: dict = None) -> Any:
    if isinstance(json, str):
        json = jsonlib.loads(json)
    r = requests.put(url, headers=_get_headers(json is not None), verify=ssl_verify, json=json, params=params)
    return _check_error(r, details)


def _check_error(r: requests.models.Response, details: str = "") -> Union[str, dict, list]:
    error, code, _ = _raise_for_status(r)
    if not error:
        try:
            data = r.json()
            return data
        except:  # NOQA
            return r.text
    if code == 412:
        raise NessiePreconidtionFailedException("Unable to complete transaction, please retry " + details, error, r)
    if code == 401:
        raise NessieUnauthorizedException("Unauthorized on api endpoint " + details, error, r)
    if code == 403:
        raise NessiePermissionException("Not permissioned to view entity at " + details, error, r)
    if code == 404:
        raise NessieNotFoundException("No entity exists at " + details, error, r)
    if code == 409:
        raise NessieConflictException("Entity already exists at " + details, error, r)
    if code == 500:
        raise NessieServerException("Server error at " + details, error, r)
    raise NessieException("unknown error", error, r)


def all_references(base_url: str, ssl_verify: bool = True) -> list:
    """Fetch all known references.

    :param base_url: base Nessie url
    :param ssl_verify: ignore ssl errors if False
    :return: json list of Nessie references
    """
    return cast(list, _get(base_url + "/trees", ssl_verify=ssl_verify))


def get_reference(base_url: str, ref: str, ssl_verify: bool = True) -> dict:
    """Fetch a reference.

    :param base_url: base Nessie url
    :param ref: name of ref to fetch
    :param ssl_verify: ignore ssl errors if False
    :return: json Nessie branch
    """
    return cast(dict, _get(base_url + "/trees/tree/{}".format(ref), ssl_verify=ssl_verify))


def create_reference(base_url: str, ref_json: dict, ssl_verify: bool = True) -> None:
    """Create a reference.

    :param base_url: base Nessie url
    :param ref: reference to create as json object
    :param ssl_verify: ignore ssl errors if False
    :return: json Nessie branch
    """
    _post(base_url + "/trees/tree", ref_json, ssl_verify=ssl_verify)


def get_default_branch(base_url: str, ssl_verify: bool = True) -> dict:
    """Fetch a reference.

    :param base_url: base Nessie url
    :param ssl_verify: ignore ssl errors if False
    :return: json Nessie branch
    """
    return cast(dict, _get(base_url + "/trees/tree", ssl_verify=ssl_verify))


def delete_branch(base_url: str, branch: str, hash_: str, reason: str = None, ssl_verify: bool = True) -> None:
    """Delete a branch.

    :param base_url: base Nessie url
    :param branch: name of branch to delete
    :param hash_: branch hash
    :param ssl_verify: ignore ssl errors if False
    """
    params = {"expectedHash": hash_}
    _delete(base_url + "/trees/branch/{}".format(branch), ssl_verify=ssl_verify, params=params)


def delete_tag(base_url: str, tag: str, hash_: str, reason: str = None, ssl_verify: bool = True) -> None:
    """Delete a tag.

    :param base_url: base Nessie url
    :param tag: name of tag to delete
    :param hash_: tag hash
    :param ssl_verify: ignore ssl errors if False
    """
    params = {"expectedHash": hash_}
    _delete(base_url + "/trees/tag/{}".format(tag), ssl_verify=ssl_verify, params=params)


def list_tables(base_url: str, ref: str, ssl_verify: bool = True) -> list:
    """Fetch a list of all tables from a known reference.

    :param base_url: base Nessie url
    :param ref: reference
    :param ssl_verify: ignore ssl errors if False
    :return: json list of Nessie table names
    """
    return cast(list, _get(base_url + "/trees/tree/{}/entries".format(ref), ssl_verify=ssl_verify))


def list_logs(base_url: str, ref: str, ssl_verify: bool = True) -> dict:
    """Fetch a list of all logs from a known starting reference.

    :param base_url: base Nessie url
    :param ref: starting reference
    :param ssl_verify: ignore ssl errors if False
    :return: json dict of Nessie logs
    """
    return cast(dict, _get(base_url + "/trees/tree/{}/log".format(ref), ssl_verify=ssl_verify))


def get_table(base_url: str, ref: str, table: str, ssl_verify: bool = True) -> dict:
    """Fetch a table from a known branch.

    :param base_url: base Nessie url
    :param ref: ref
    :param table: name of table
    :param ssl_verify: ignore ssl errors if False
    :return: json dict of Nessie table
    """
    params = {"ref": ref}
    return cast(dict, _get(base_url + "/contents/{}".format(table), ssl_verify=ssl_verify, params=params))


def assign_branch(base_url: str, branch: str, branch_json: dict, old_hash: str, ssl_verify: bool = True) -> None:
    """Assign a reference to a branch.

    :param base_url: base Nessie url
    :param branch: name of the branch
    :param branch_json: new definition of the branch
    :param old_hash: current hash of the branch
    :param ssl_verify: ignore ssl errors if False
    """
    url = "/trees/branch/{}".format(branch)
    params = {"expectedHash": old_hash}
    _put(base_url + url, branch_json, ssl_verify=ssl_verify, params=params)


def assign_tag(base_url: str, tag: str, tag_json: dict, old_hash: str, ssl_verify: bool = True) -> None:
    """Assign a reference to a tag.

    :param base_url: base Nessie url
    :param tag: name of the tag
    :param tag_json: new definition of the tag
    :param old_hash: current hash of the tag
    :param ssl_verify: ignore ssl errors if False
    """
    url = "/trees/tag/{}".format(tag)
    params = {"expectedHash": old_hash}
    _put(base_url + url, tag_json, ssl_verify=ssl_verify, params=params)


def _raise_for_status(self: requests.models.Response) -> Tuple[Union[HTTPError, None], int, Union[Any, str]]:
    """Raises stored :class:`HTTPError`, if one occurred. Copy from requests request.raise_for_status()."""
    http_error_msg = ""
    if isinstance(self.reason, bytes):
        try:
            reason = self.reason.decode("utf-8")
        except UnicodeDecodeError:
            reason = self.reason.decode("iso-8859-1")
    else:
        reason = self.reason

    if 400 <= self.status_code < 500:
        http_error_msg = u"%s Client Error: %s for url: %s" % (self.status_code, reason, self.url)

    elif 500 <= self.status_code < 600:
        http_error_msg = u"%s Server Error: %s for url: %s" % (self.status_code, reason, self.url)

    if http_error_msg:
        return HTTPError(http_error_msg, response=self), self.status_code, reason
    else:
        return None, self.status_code, reason


def cherry_pick(base_url: str, branch: str, transplant_json: dict, expected_hash: str, ssl_verify: bool = True) -> None:
    """cherry-pick a list of hashes to a branch.

    :param base_url: base Nessie url
    :param branch: name of branch to cherry pick onto
    :param transplant_json: transplant content
    :param expected_hash: expected hash of HEAD of branch
    :param ssl_verify: ignore ssl errors if False
    """
    url = "/trees/branch/{}/transplant".format(branch)
    params = {"expectedHash": expected_hash}
    _post(base_url + url, json=transplant_json, ssl_verify=ssl_verify, params=params)


def merge(base_url: str, branch: str, merge_json: dict, expected_hash: str, ssl_verify: bool = True) -> None:
    """Merge a branch into another branch.

    :param base_url: base Nessie url
    :param branch: name of branch to merge onto
    :param merge_json: merge content
    :param expected_hash: expected hash of HEAD of branch
    :param ssl_verify: ignore ssl errors if False
    """
    url = "/trees/branch/{}/merge".format(branch)
    params = {"expectedHash": expected_hash}
    _post(base_url + url, json=merge_json, ssl_verify=ssl_verify, params=params)


def commit(
    base_url: str,
    branch: str,
    operations: str,
    expected_hash: str,
    reason: Optional[str],
    ssl_verify: bool = True,
) -> None:
    """Commit a set of operations to a branch.

    :param base_url: base Nessie url
    :param branch: name of branch to merge onto
    :param operations: json object of operations
    :param reason: commit message
    :param expected_hash: expected hash of HEAD of branch
    :param ssl_verify: ignore ssl errors if False
    """
    url = "/trees/branch/{}/commit".format(branch)
    params = {"expectedHash": expected_hash}
    if reason:
        params["message"] = reason
    _post(base_url + url, json=operations, ssl_verify=ssl_verify, params=params)
