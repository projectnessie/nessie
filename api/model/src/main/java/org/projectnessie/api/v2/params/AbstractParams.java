/*
 * Copyright (C) 2022 Dremio
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
package org.projectnessie.api.v2.params;

import static org.projectnessie.api.v2.doc.ApiDoc.MAX_RECORDS;
import static org.projectnessie.api.v2.doc.ApiDoc.PAGE_TOKEN;

import javax.annotation.Nullable;
import javax.ws.rs.QueryParam;
import org.eclipse.microprofile.openapi.annotations.parameters.Parameter;

public abstract class AbstractParams<IMPL extends AbstractParams<IMPL>> implements AbstractSpec {

  @Parameter(description = MAX_RECORDS)
  @QueryParam("max-records")
  @jakarta.ws.rs.QueryParam("max-records")
  @Nullable
  @jakarta.annotation.Nullable
  private Integer maxRecords;

  @Parameter(description = PAGE_TOKEN)
  @QueryParam("page-token")
  @jakarta.ws.rs.QueryParam("page-token")
  @Nullable
  @jakarta.annotation.Nullable
  private String pageToken;

  protected AbstractParams() {}

  protected AbstractParams(@Nullable Integer maxRecords, @Nullable String pageToken) {
    this.maxRecords = maxRecords;
    this.pageToken = pageToken;
  }

  @Nullable
  @jakarta.annotation.Nullable
  @Override
  public Integer maxRecords() {
    return maxRecords;
  }

  @Nullable
  @jakarta.annotation.Nullable
  @Override
  public String pageToken() {
    return pageToken;
  }

  public abstract IMPL forNextPage(String pageToken);

  public abstract static class Builder<T extends Builder<T>> {

    protected Integer maxRecords;
    protected String pageToken;

    protected Builder() {}

    @SuppressWarnings("unchecked")
    public T maxRecords(Integer maxRecords) {
      this.maxRecords = maxRecords;
      return (T) this;
    }

    @SuppressWarnings("unchecked")
    public T pageToken(String pageToken) {
      this.pageToken = pageToken;
      return (T) this;
    }
  }
}
