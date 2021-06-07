/*
 * Copyright (C) 2020 Dremio
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.projectnessie.api.params;


import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;

import javax.ws.rs.QueryParam;

import org.eclipse.microprofile.openapi.annotations.media.ExampleObject;
import org.eclipse.microprofile.openapi.annotations.parameters.Parameter;

/**
 * The purpose of this class is to include optional parameters that can be passed to
 * {@link org.projectnessie.api.TreeApi#getCommitLog(String, CommitLogParams)}.
 *
 * <p>For easier usage of this class, there is {@link CommitLogParams#builder()}, which allows configuring/setting the different parameters.
 */
public class CommitLogParams {

  @Parameter(description = "maximum number of commit-log entries to return, just a hint for the server")
  @QueryParam("max")
  private Integer maxRecords;

  @Parameter(description = "pagination continuation token, as returned in the previous LogResponse.token")
  @QueryParam("pageToken")
  private String pageToken;

  @Parameter(description = "List of authors to filter by. The author is the original committer. "
      + "No filtering by author will happen if this is set to null/empty")
  @QueryParam("authors")
  private List<String> authors;

  @Parameter(description = "List of committers to filter by. This is the logged in user/account who performed the commit. "
      + "No filtering by committer will happen if this is set to null/empty")
  @QueryParam("committers")
  private List<String> committers;

  @Parameter(description = "Only include commits newer than the specified date in ISO-8601 format. "
      + "No filtering will happen if this is set to null", examples = {@ExampleObject(ref = "javaInstant")})
  @QueryParam("after")
  private Instant after;

  @Parameter(description = "Only include commits older than the specified date in ISO-8601 format. "
      + "No filtering will happen if this is set to null", examples = {@ExampleObject(ref = "javaInstant")})
  @QueryParam("before")
  private Instant before;

  public CommitLogParams() {
  }

  private CommitLogParams(Integer maxRecords, String pageToken, List<String> authors, List<String> committers, Instant after,
      Instant before) {
    this.maxRecords = maxRecords;
    this.pageToken = pageToken;
    this.authors = authors;
    this.committers = committers;
    this.after = after;
    this.before = before;
  }

  private CommitLogParams(Builder builder) {
    this(builder.maxRecords, builder.pageToken, new ArrayList<>(builder.authors),
        new ArrayList<>(builder.committers), builder.after, builder.before);
  }

  public Integer getMaxRecords() {
    return maxRecords;
  }

  public String getPageToken() {
    return pageToken;
  }

  public List<String> getAuthors() {
    return authors;
  }

  public List<String> getCommitters() {
    return committers;
  }

  public Instant getAfter() {
    return after;
  }

  public Instant getBefore() {
    return before;
  }

  public static CommitLogParams.Builder builder() {
    return new CommitLogParams.Builder();
  }

  public static CommitLogParams empty() {
    return new CommitLogParams.Builder().build();
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", CommitLogParams.class.getSimpleName() + "[", "]")
        .add("maxRecords=" + maxRecords)
        .add("pageToken='" + pageToken + "'")
        .add("authors=" + authors)
        .add("committers=" + committers)
        .add("after=" + after)
        .add("before=" + before)
        .toString();
  }

  public static class Builder {

    private Integer maxRecords;
    private String pageToken;
    private List<String> authors = Collections.emptyList();
    private List<String> committers = Collections.emptyList();
    private Instant after;
    private Instant before;

    private Builder() {
    }

    public Builder maxRecords(Integer maxRecords) {
      this.maxRecords = maxRecords;
      return this;
    }

    public Builder pageToken(String pageToken) {
      this.pageToken = pageToken;
      return this;
    }

    public Builder authors(List<String> authors) {
      this.authors = authors;
      return this;
    }

    public Builder committers(List<String> committers) {
      this.committers = committers;
      return this;
    }

    public Builder after(Instant after) {
      this.after = after;
      return this;
    }

    public Builder before(Instant before) {
      this.before = before;
      return this;
    }

    public Builder from(CommitLogParams params) {
      return maxRecords(params.maxRecords).pageToken(params.pageToken).authors(params.authors).committers(params.committers)
          .after(params.after).before(params.before);
    }

    private void validate() {
      Objects.requireNonNull(authors, "authors must be non-null");
      Objects.requireNonNull(committers, "committers must be non-null");
    }

    public CommitLogParams build() {
      validate();
      return new CommitLogParams(this);
    }
  }
}
