# -*- coding: utf-8 -*-
"""Makes sure the expression building functions work properly."""
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

from assertpy import assert_that

from pynessie.utils import build_filter_for_commit_log_flags
from pynessie.utils import build_filter_for_contents_listing_flags
from pynessie.utils import parse_to_iso8601


def test_building_empty_filter() -> None:
    """Makes sure the expression building functions work with empty input."""
    assert_that(build_filter_for_commit_log_flags(query_filter="", authors=[], committers=[], since=None, until=None)).is_none()

    assert_that(build_filter_for_contents_listing_flags(query_filter="", types=[])).is_none()


def test_building_filter_for_commit_log() -> None:
    """Makes sure the filter building function for the commit log produces what we expect."""
    authors = []
    committers = []
    since = None
    until = None
    query_expr = ""
    assert_that(
        build_filter_for_commit_log_flags(
            query_filter="some_exclusive_filter", authors=authors, committers=committers, since=since, until=until
        )
    ).is_equal_to("some_exclusive_filter")

    assert_that(
        build_filter_for_commit_log_flags(
            query_filter="some_exclusive_filter", authors=["one"], committers=["two"], since=since, until=until
        )
    ).is_equal_to("some_exclusive_filter")

    assert_that(
        build_filter_for_commit_log_flags(query_filter=query_expr, authors=["one"], committers=committers, since=since, until=until)
    ).is_equal_to("commit.author=='one'")

    assert_that(
        build_filter_for_commit_log_flags(
            query_filter=query_expr, authors=["one", "two", "three"], committers=committers, since=since, until=until
        )
    ).is_equal_to("(commit.author=='one' || commit.author=='two' || commit.author=='three')")

    assert_that(
        build_filter_for_commit_log_flags(query_filter=query_expr, authors=["one"], committers=["two"], since=since, until=until)
    ).is_equal_to("(commit.author=='one' && commit.committer=='two')")

    assert_that(
        build_filter_for_commit_log_flags(
            query_filter=query_expr, authors=["one", "two"], committers=["two", "three"], since=since, until=until
        )
    ).is_equal_to("((commit.author=='one' || commit.author=='two') && (commit.committer=='two' || commit.committer=='three'))")

    assert_that(
        build_filter_for_commit_log_flags(query_filter=query_expr, authors=[], committers=[], since="2021-05-31T08:23:15Z", until=until)
    ).is_equal_to("timestamp(commit.commitTime) > timestamp('2021-05-31T08:23:15+00:00')")

    assert_that(
        build_filter_for_commit_log_flags(query_filter=query_expr, authors=[], committers=[], since="", until="2021-05-31T10:23:15Z")
    ).is_equal_to("timestamp(commit.commitTime) < timestamp('2021-05-31T10:23:15+00:00')")

    assert_that(
        build_filter_for_commit_log_flags(
            query_filter=query_expr, authors=[], committers=[], since="2021-05-31T08:23:15Z", until="2021-05-31T10:23:15Z"
        )
    ).is_equal_to(
        "(timestamp(commit.commitTime) > timestamp('2021-05-31T08:23:15+00:00') "
        "&& timestamp(commit.commitTime) < timestamp('2021-05-31T10:23:15+00:00'))"
    )

    assert_that(
        build_filter_for_commit_log_flags(
            query_filter=query_expr,
            authors=["one", "two"],
            committers=["two", "three"],
            since="2021-05-31T08:23:15Z",
            until="2021-05-31T10:23:15Z",
        )
    ).is_equal_to(
        "((commit.author=='one' || commit.author=='two') "
        "&& (commit.committer=='two' || commit.committer=='three') "
        "&& timestamp(commit.commitTime) > timestamp('2021-05-31T08:23:15+00:00') "
        "&& timestamp(commit.commitTime) < timestamp('2021-05-31T10:23:15+00:00'))"
    )


def test_building_filter_for_content_entries() -> None:
    """Makes sure the expression building function for the content entries produces what we expect."""
    assert_that(build_filter_for_contents_listing_flags(query_filter="exclusive_expr", types=[])).is_equal_to("exclusive_expr")

    assert_that(
        build_filter_for_contents_listing_flags(query_filter="exclusive_expr", types=["ICEBERG_VIEW", "DELTA_LAKE_TABLE"])
    ).is_equal_to("exclusive_expr")

    assert_that(build_filter_for_contents_listing_flags(query_filter="", types=["ICEBERG_TABLE"])).is_equal_to(
        "entry.contentType in ['ICEBERG_TABLE']"
    )
    assert_that(build_filter_for_contents_listing_flags(query_filter="", types=["ICEBERG_TABLE", "DELTA_LAKE_TABLE"])).is_equal_to(
        "entry.contentType in ['ICEBERG_TABLE','DELTA_LAKE_TABLE']"
    )


def test_date_parsing() -> None:
    """Tests date parsing."""
    assert_that(parse_to_iso8601("2021-05-31 08:23:15Z")).is_equal_to("2021-05-31T08:23:15+00:00")
    assert_that(parse_to_iso8601("2021-05-31T08:23:15+01:00")).is_equal_to("2021-05-31T07:23:15+00:00")
    assert_that(parse_to_iso8601("2021-05-31T08:23:15+02:00")).is_equal_to("2021-05-31T06:23:15+00:00")
    assert_that(parse_to_iso8601("2021-05-31 08:23:15Z+00:00")).is_equal_to("2021-05-31T08:23:15+00:00")
    assert_that(parse_to_iso8601("2021-06-30T08:19:04.826051Z")).is_equal_to("2021-06-30T08:19:04.826051+00:00")


def test_invalid_date_parsing() -> None:
    """Tests invalid date parsing."""
    assert_that(parse_to_iso8601).raises(ValueError).when_called_with(None)
    assert_that(parse_to_iso8601).raises(ValueError).when_called_with("")
    assert_that(parse_to_iso8601).raises(ValueError).when_called_with("some_invalid_str")
    assert_that(parse_to_iso8601).raises(ValueError).when_called_with("99999")
