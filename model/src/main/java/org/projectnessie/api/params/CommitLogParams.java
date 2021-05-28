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
import java.util.StringJoiner;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;

import org.eclipse.microprofile.openapi.annotations.media.ExampleObject;
import org.eclipse.microprofile.openapi.annotations.parameters.Parameter;
import org.projectnessie.model.Validation;

public class CommitLogParams {

  @NotNull
  @Pattern(regexp = Validation.REF_NAME_OR_HASH_REGEX, message = Validation.REF_NAME_OR_HASH_MESSAGE)
  @Parameter(description = "ref to show log from", examples = {@ExampleObject(ref = "ref")})
  @PathParam("ref")
  private String ref;

  @Parameter(description = "maximum number of commit-log entries to return, just a hint for the server")
  @QueryParam("max")
  private Integer maxRecords;

  @Parameter(description = "pagination continuation token, as returned in the previous LogResponse.token")
  @QueryParam("pageToken")
  private String pageToken;

  @Parameter(description = "The author of a commit. This is the original committer. "
      + "No filtering by author will happen if this is set to null")
  @QueryParam("author")
  private String author;

  @Parameter(description = "The logged in user/account who performed the commit. "
      + "No filtering by committer will happen if this is set to null")
  @QueryParam("committer")
  private String committer;

  @Parameter(description = "Only include commits newer than the specified date in ISO-8601 format. "
      + "No filtering will happen if this is set to null")
  @QueryParam("after")
  private Instant after;

  @Parameter(description = "Only include commits older than the specified date in ISO-8601 format. "
      + "No filtering will happen if this is set to null")
  @QueryParam("before")
  private Instant before;

  public CommitLogParams() {
  }

  private CommitLogParams(String ref, Integer maxRecords, String pageToken, String author, String committer, Instant after,
      Instant before) {
    this.ref = ref;
    this.maxRecords = maxRecords;
    this.pageToken = pageToken;
    this.author = author;
    this.committer = committer;
    this.after = after;
    this.before = before;
  }

  private CommitLogParams(Builder builder) {
    this(builder.ref, builder.maxRecords, builder.pageToken, builder.author, builder.committer, builder.after, builder.before);
  }

  public String getRef() {
    return ref;
  }

  public Integer getMaxRecords() {
    return maxRecords;
  }

  public String getPageToken() {
    return pageToken;
  }

  public String getAuthor() {
    return author;
  }

  public String getCommitter() {
    return committer;
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

  @Override
  public String toString() {
    return new StringJoiner(", ", CommitLogParams.class.getSimpleName() + "[", "]")
        .add("ref='" + ref + "'")
        .add("maxRecords=" + maxRecords)
        .add("pageToken='" + pageToken + "'")
        .add("author='" + author + "'")
        .add("committer='" + committer + "'")
        .add("after='" + after + "'")
        .add("before='" + before + "'")
        .toString();
  }

  public static class Builder {

    private String ref;
    private Integer maxRecords;
    private String pageToken;
    private String author;
    private String committer;
    private Instant after;
    private Instant before;

    private Builder() {
    }

    public Builder ref(String ref) {
      this.ref = ref;
      return this;
    }

    public Builder maxRecords(Integer maxRecords) {
      this.maxRecords = maxRecords;
      return this;
    }

    public Builder pageToken(String pageToken) {
      this.pageToken = pageToken;
      return this;
    }

    public Builder author(String author) {
      this.author = author;
      return this;
    }

    public Builder committer(String committer) {
      this.committer = committer;
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
      return ref(params.ref).maxRecords(params.maxRecords).pageToken(params.pageToken).author(params.author).committer(params.committer)
          .after(params.after).before(params.before);
    }

    public CommitLogParams build() {
      return new CommitLogParams(this);
    }
  }
}
