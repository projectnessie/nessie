# -*- coding: utf-8 -*-
"""Direct API operations on Nessie with requests."""
from typing import Any
from typing import cast
from typing import Tuple
from typing import Union

import requests
import simplejson as jsonlib
from requests.auth import AuthBase
from requests.exceptions import HTTPError

from .error import NessieConflictException
from .error import NessieException
from .error import NessieNotFoundException
from .error import NessiePermissionException
from .error import NessiePreconidtionFailedException
from .error import NessieUnauthorizedException
from .model import BranchSchema


def _get_headers(version: str = None) -> dict:
    headers = {"Content-Type": "application/json"}
    if version:
        headers["If-Match"] = '"{}"'.format(version)
    return headers


def _get(
    url: str, token: AuthBase, details: str = "", ssl_verify: bool = True, params: dict = None
) -> Union[str, dict, list]:
    r = requests.get(url, headers=_get_headers(), verify=ssl_verify, params=params, auth=token)
    return _check_error(r, details)


def _post(
    url: str,
    token: AuthBase,
    json: dict = None,
    details: str = "",
    ssl_verify: bool = True,
    params: dict = None,
    version: str = None,
) -> Union[str, dict, list]:
    if isinstance(json, str):
        json = jsonlib.loads(json)
    r = requests.post(url, headers=_get_headers(version), verify=ssl_verify, json=json, params=params, auth=token)
    return _check_error(r, details)


def _delete(
    url: str, token: AuthBase, details: str = "", ssl_verify: bool = True, params: dict = None, version: str = None
) -> Union[str, dict, list]:
    r = requests.delete(url, headers=_get_headers(version), verify=ssl_verify, params=params, auth=token)
    return _check_error(r, details)


def _put(
    url: str,
    token: AuthBase,
    json: dict = None,
    details: str = "",
    ssl_verify: bool = True,
    params: dict = None,
    version: str = None,
) -> Union[str, dict, list]:
    if isinstance(json, str):
        json = jsonlib.loads(json)
    r = requests.put(url, headers=_get_headers(version), verify=ssl_verify, json=json, params=params, auth=token)
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
    raise NessieException("unknown error", error)


def all_branches(token: AuthBase, base_url: str, ssl_verify: bool = True) -> list:
    """Fetch all known branches.

    :param token: auth token from previous login attempt
    :param base_url: base Nessie url
    :param ssl_verify: ignore ssl errors if False
    :return: json list of Nessie branches
    """
    return cast(list, _get(base_url + "/objects", token, ssl_verify=ssl_verify))


def get_branch(token: AuthBase, base_url: str, branch: str, ssl_verify: bool = True) -> dict:
    """Fetch a branch.

    :param token: auth token from previous login attempt
    :param base_url: base Nessie url
    :param branch: name of branch to fetch
    :param ssl_verify: ignore ssl errors if False
    :return: json Nessie branch
    """
    return cast(dict, _get(base_url + "/objects/{}".format(branch), token, ssl_verify=ssl_verify))


def delete_branch(
    token: AuthBase, base_url: str, branch: str, version: str, reason: str = None, ssl_verify: bool = True
) -> None:
    """Delete a branch.

    :param token: auth token from previous login attempt
    :param base_url: base Nessie url
    :param branch: name of branch to delete
    :param version: current version for concurrency control
    :param reason: why this action is being performed (for log)
    :param ssl_verify: ignore ssl errors if False
    """
    _delete(
        base_url + "/objects/{}".format(branch),
        token,
        ssl_verify=ssl_verify,
        params={"reason": reason},
        version=version,
    )


def merge_branch(
    token: AuthBase,
    base_url: str,
    to_branch: str,
    from_branch: str,
    version: str,
    force: bool = False,
    reason: str = None,
    ssl_verify: bool = True,
) -> None:
    """Merge a branch from_branch into to_branch.

    :param token: auth token from previous login attempt
    :param base_url: base Nessie url
    :param to_branch: name of branch to merge to
    :param from_branch: name of branch to merge from
    :param version: current version for concurrency control
    :param force: force merge
    :param reason: why this action is being performed (for log)
    :param ssl_verify: ignore ssl errors if False
    """
    _put(
        base_url + "/objects/{}/promote".format(to_branch),
        token,
        ssl_verify=ssl_verify,
        params={"promote": from_branch, "reason": reason, "force": force},
        version=version,
    )


def list_tables(token: AuthBase, base_url: str, branch: str, namespace: str = None, ssl_verify: bool = True) -> list:
    """Fetch a list of all tables from a known branch.

    :param token: auth token from previous login attempt
    :param base_url: base Nessie url
    :param branch: name of branch
    :param namespace: optional namespace of
    :param ssl_verify: ignore ssl errors if False
    :return: json list of Nessie table names
    """
    params = {"namespace": namespace} if namespace else None
    return cast(list, _get(base_url + "/objects/{}/tables".format(branch), token, ssl_verify=ssl_verify, params=params))


def get_table(token: AuthBase, base_url: str, branch: str, table: str, ssl_verify: bool = True) -> dict:
    """Fetch a table from a known branch.

    :param token: auth token from previous login attempt
    :param base_url: base Nessie url
    :param branch: name of branch
    :param table: name of table
    :param ssl_verify: ignore ssl errors if False
    :return: json dict of Nessie table
    """
    return cast(dict, _get(base_url + "/objects/{}/{}".format(branch, table), token, ssl_verify=ssl_verify))


def create_branch(
    token: AuthBase, base_url: str, branch: str, base_branch: str = None, reason: str = None, ssl_verify: bool = True
) -> None:
    """Fetch all known branches.

    :param token: auth token from previous login attempt
    :param base_url: base Nessie url
    :param branch: name of new branch
    :param base_branch: name of branch to fork from
    :param reason: why this action is being performed (for log)
    :param ssl_verify: ignore ssl errors if False
    """
    branch_obj = dict(name=branch)
    branch_obj["id"] = cast(str, base_branch)

    _post(
        base_url + "/objects/{}".format(branch),
        token,
        BranchSchema().dump(branch_obj),
        ssl_verify=ssl_verify,
        params={"reason": reason},
    )


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
