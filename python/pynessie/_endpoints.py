# -*- coding: utf-8 -*-
"""Direct API operations on Nessie with requests."""
from typing import Any
from typing import cast
from typing import Optional
from typing import Tuple
from typing import Union

import requests
import simplejson as jsonlib
from confuse import Configuration
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


class EndPoints(object):
    """Configuration and method for accessing REST endpoints."""

    def __init__(self: "EndPoints", config: Configuration) -> None:
        """Create a Nessie Client from known config."""
        self._base_url = config["endpoint"].get()
        self._ssl_verify = config["verify"].get(bool)
        self._auth = None

    def _get(self: "EndPoints", rel_url: str, details: str = "", params: dict = None) -> Union[str, dict, list]:
        r = requests.get(self._base_url + rel_url, headers=_get_headers(), verify=self._ssl_verify, params=params, auth=self._auth)
        return _check_error(r, details)

    def _post(
        self: "EndPoints", rel_url: str, json: Union[str, dict] = None, details: str = "", ssl_verify: bool = True, params: dict = None
    ) -> Union[str, dict, list]:
        if isinstance(json, str):
            json = jsonlib.loads(json)
        r = requests.post(self._base_url + rel_url, headers=_get_headers(json is not None), verify=ssl_verify, json=json, params=params)
        return _check_error(r, details)

    def _delete(self: "EndPoints", rel_url: str, details: str = "", ssl_verify: bool = True, params: dict = None) -> Union[str, dict, list]:
        r = requests.delete(self._base_url + rel_url, headers=_get_headers(), verify=ssl_verify, params=params)
        return _check_error(r, details)

    def _put(
        self: "EndPoints", rel_url: str, json: Union[str, dict] = None, details: str = "", ssl_verify: bool = True, params: dict = None
    ) -> Any:
        if isinstance(json, str):
            json = jsonlib.loads(json)
        r = requests.put(self._base_url + rel_url, headers=_get_headers(json is not None), verify=ssl_verify, json=json, params=params)
        return _check_error(r, details)

    def all_references(self: "EndPoints") -> list:
        """Fetch all known references.

        :return: json list of Nessie references
        """
        return cast(list, self._get("/trees"))

    def get_reference(self: "EndPoints", ref: str) -> dict:
        """Fetch a reference.

        :param ref: name of ref to fetch
        :return: json Nessie branch or tag
        """
        return cast(dict, self._get("/trees/tree/{}".format(ref)))

    def create_reference(self: "EndPoints", ref_json: dict) -> dict:
        """Create a reference.

        :param ref_json: reference to create as json object
        :return: json Nessie branch or tag
        """
        return cast(dict, self._post("/trees/tree", ref_json))

    def get_default_branch(self: "EndPoints") -> dict:
        """Fetch a reference.

        :return: json Nessie branch
        """
        return cast(dict, self._get("/trees/tree"))

    def delete_branch(self: "EndPoints", branch: str, hash_: str, reason: str = None) -> None:
        """Delete a branch.

        :param branch: name of branch to delete
        :param hash_: branch hash
        """
        params = {"expectedHash": hash_}
        self._delete("/trees/branch/{}".format(branch), params=params)

    def delete_tag(self: "EndPoints", tag: str, hash_: str, reason: str = None) -> None:
        """Delete a tag.

        :param tag: name of tag to delete
        :param hash_: tag hash
        """
        params = {"expectedHash": hash_}
        self._delete("/trees/tag/{}".format(tag), params=params)

    def list_tables(
        self: "EndPoints",
        ref: str,
        max_result_hint: Optional[int] = None,
        page_token: Optional[str] = None,
        query_expression: Optional[str] = None,
    ) -> list:
        """Fetch a list of all tables from a known reference.

        :param ref: reference
        :param max_result_hint: hint for the server, maximum number of results to return
        :param page_token: the token retrieved from a previous page returned for the same ref
        :param query_expression: A CEL expression that allows advanced filtering capabilities
        :return: json list of Nessie table names
        """
        params = dict()
        if max_result_hint:
            params["max"] = str(max_result_hint)
        if page_token:
            params["pageToken"] = page_token
        if query_expression:
            params["query_expression"] = query_expression
        return cast(list, self._get("/trees/tree/{}/entries".format(ref), params=params))

    def list_logs(self: "EndPoints", ref: str, **filtering_args: Any) -> dict:
        """Fetch a list of all logs from a known starting reference.

        :param ref: starting reference
        :param filtering_args: All of the args used to filter the log
        :return: json dict of Nessie logs
        """
        return cast(dict, self._get("/trees/tree/{}/log".format(ref), params=filtering_args))

    def get_table(self: "EndPoints", ref: str, table: str) -> dict:
        """Fetch a table from a known branch.

        :param ref: ref
        :param table: name of table
        :return: json dict of Nessie table
        """
        params = {"ref": ref}
        return cast(dict, self._get("/contents/{}".format(table), params=params))

    def assign_branch(self: "EndPoints", branch: str, branch_json: dict, old_hash: str) -> None:
        """Assign a reference to a branch.

        :param branch: name of the branch
        :param branch_json: new definition of the branch
        :param old_hash: current hash of the branch
        """
        url = "/trees/branch/{}".format(branch)
        params = {"expectedHash": old_hash}
        self._put(url, branch_json, params=params)

    def assign_tag(self: "EndPoints", tag: str, tag_json: dict, old_hash: str) -> None:
        """Assign a reference to a tag.

        :param tag: name of the tag
        :param tag_json: new definition of the tag
        :param old_hash: current hash of the tag
        """
        url = "/trees/tag/{}".format(tag)
        params = {"expectedHash": old_hash}
        self._put(url, tag_json, params=params)

    def cherry_pick(self: "EndPoints", branch: str, transplant_json: dict, expected_hash: str) -> None:
        """cherry-pick a list of hashes to a branch.

        :param branch: name of branch to cherry pick onto
        :param transplant_json: transplant content
        :param expected_hash: expected hash of HEAD of branch
        """
        url = "/trees/branch/{}/transplant".format(branch)
        params = {"expectedHash": expected_hash}
        self._post(url, json=transplant_json, params=params)

    def merge(self: "EndPoints", branch: str, merge_json: dict, expected_hash: str) -> None:
        """Merge a branch into another branch.

        :param branch: name of branch to merge onto
        :param merge_json: merge content
        :param expected_hash: expected hash of HEAD of branch
        """
        url = "/trees/branch/{}/merge".format(branch)
        params = {"expectedHash": expected_hash}
        self._post(url, json=merge_json, params=params)

    def commit(
        self: "EndPoints",
        branch: str,
        operations: str,
        expected_hash: str,
    ) -> dict:
        """Commit a set of operations to a branch.

        :param branch: name of branch to merge onto
        :param operations: json object of operations
        :param expected_hash: expected hash of HEAD of branch
        """
        url = "/trees/branch/{}/commit".format(branch)
        params = {"expectedHash": expected_hash}
        return cast(dict, self._post(url, json=operations, params=params))
