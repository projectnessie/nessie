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

"""The purpose of this module is to provide different functions that can produce CEL Expressions based on given filtering parameters."""


from datetime import timezone
from typing import List, Optional

from dateutil import parser


def build_filter_for_commit_log_flags(
    query_filter: str, authors: List[str], committers: List[str], since: Optional[str], until: Optional[str]
) -> Optional[str]:
    """Producs a CEL expression to be used for filtering the commit log based on the given filtering parameters."""
    if query_filter:
        return query_filter
    expressions = []

    if authors:
        expressions.append(_expression_for_commit_log_by_author(authors))
    if committers:
        expressions.append(_expression_for_commit_log_by_committer(committers))
    if since:
        expressions.append(_expression_for_commit_log_by_commit_time_after(parse_to_iso8601(since)))
    if until:
        expressions.append(_expression_for_commit_log_by_commit_time_before(parse_to_iso8601(until)))

    if len(expressions) > 0:
        return _and_join(expressions)

    return None


def build_filter_for_contents_listing_flags(query_filter: str, types: List[str]) -> Optional[str]:
    """Producs a CEL expression to be used for filtering the content entries based on the given filtering parameters."""
    if query_filter:
        return query_filter

    if types:
        return _expression_for_entries_by_types(types)

    return None


def _expression_for_commit_log_by_author(authors: List[str]) -> str:
    return __generate_expression(authors, "commit.author=='{}'")


def _expression_for_commit_log_by_committer(committers: List[str]) -> str:
    return __generate_expression(committers, "commit.committer=='{}'")


def _expression_for_commit_log_by_commit_time_after(commit_time_after: str) -> str:
    return "timestamp(commit.commitTime) > timestamp('{}')".format(commit_time_after)


def _expression_for_commit_log_by_commit_time_before(commit_time_before: str) -> str:
    return "timestamp(commit.commitTime) < timestamp('{}')".format(commit_time_before)


def _expression_for_entries_by_types(types: List[str]) -> str:
    return "entry.contentType in [{}]".format(",".join(["'" + t + "'" for t in types]))


def _and_join(expressions: List[str]) -> str:
    result = " && ".join(expressions)
    if len(expressions) > 1:
        result = "(" + result + ")"
    return result


def _or_join(expressions: List[str]) -> str:
    result = " || ".join(expressions)
    if len(expressions) > 1:
        result = "(" + result + ")"
    return result


def __generate_expression(items: List[str], expression_template: str) -> str:
    expressions = []
    for item in items:
        expressions.append(expression_template.format(item))
    return _or_join(expressions)


def parse_to_iso8601(date_str: str) -> str:
    """Parses a given Date string to an ISO 8601 compliant string in UTC.

    :param date_str A string representation of a date
    :return An ISO 8601 string in the form YYYY-MM-DD HH:MM:SS.mmmmmm+HH:MM
    """
    try:
        return parser.parse(date_str).astimezone(tz=timezone.utc).isoformat(sep="T")
    except Exception as e:
        raise ValueError("Unable to parse '{}' to an ISO 8601-compliant string. Reason: {}".format(date_str, e), e) from e
