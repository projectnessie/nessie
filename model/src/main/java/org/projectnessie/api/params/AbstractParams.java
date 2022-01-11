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

import javax.annotation.Nullable;
import javax.ws.rs.QueryParam;
import org.eclipse.microprofile.openapi.annotations.parameters.Parameter;

public abstract class AbstractParams {

  @Parameter(description = "maximum number of entries to return, just a hint for the server")
  @QueryParam("maxRecords")
  @Nullable
  private Integer maxRecords;

  @Parameter(
      description =
          "paging continuation token, as returned in the previous value of the field 'token' in "
              + "the corresponding 'EntriesResponse' or 'LogResponse' or 'ReferencesResponse' or 'RefLogResponse'.")
  @QueryParam("pageToken")
  @Nullable
  private String pageToken;

  protected AbstractParams() {}

  protected AbstractParams(Integer maxRecords, String pageToken) {
    this.maxRecords = maxRecords;
    this.pageToken = pageToken;
  }

  public Integer maxRecords() {
    return maxRecords;
  }

  public String pageToken() {
    return pageToken;
  }

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
