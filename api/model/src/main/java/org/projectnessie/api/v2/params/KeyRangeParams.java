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

import static org.projectnessie.api.v2.doc.ApiDoc.KEY_MAX_PARAMETER_DESCRIPTION;
import static org.projectnessie.api.v2.doc.ApiDoc.KEY_MIN_PARAMETER_DESCRIPTION;
import static org.projectnessie.api.v2.doc.ApiDoc.KEY_PREFIX_PARAMETER_DESCRIPTION;

import javax.annotation.Nullable;
import javax.ws.rs.QueryParam;
import org.eclipse.microprofile.openapi.annotations.media.ExampleObject;
import org.eclipse.microprofile.openapi.annotations.parameters.Parameter;
import org.projectnessie.model.ContentKey;

public abstract class KeyRangeParams<IMPL extends KeyRangeParams<IMPL>>
    extends AbstractParams<IMPL> {

  @Parameter(
      description = KEY_MIN_PARAMETER_DESCRIPTION,
      examples = @ExampleObject(ref = "ContentKeyGet"))
  @QueryParam("min-key")
  @jakarta.ws.rs.QueryParam("min-key")
  @Nullable
  @jakarta.annotation.Nullable
  private ContentKey minKey;

  @Parameter(
      description = KEY_MAX_PARAMETER_DESCRIPTION,
      examples = @ExampleObject(ref = "ContentKeyGet"))
  @QueryParam("max-key")
  @jakarta.ws.rs.QueryParam("max-key")
  @Nullable
  @jakarta.annotation.Nullable
  private ContentKey maxKey;

  @Parameter(
      description = KEY_PREFIX_PARAMETER_DESCRIPTION,
      examples = @ExampleObject(ref = "ContentKeyGet"))
  @QueryParam("prefix-key")
  @jakarta.ws.rs.QueryParam("prefix-key")
  @Nullable
  @jakarta.annotation.Nullable
  private ContentKey prefixKey;

  protected KeyRangeParams() {}

  public KeyRangeParams(
      @Nullable @jakarta.annotation.Nullable Integer maxRecords,
      @Nullable @jakarta.annotation.Nullable String pageToken,
      @Nullable @jakarta.annotation.Nullable ContentKey minKey,
      @Nullable @jakarta.annotation.Nullable ContentKey maxKey,
      @Nullable @jakarta.annotation.Nullable ContentKey prefixKey) {
    super(maxRecords, pageToken);
    this.minKey = minKey;
    this.maxKey = maxKey;
    this.prefixKey = prefixKey;
  }

  @Nullable
  @jakarta.annotation.Nullable
  public ContentKey minKey() {
    return minKey;
  }

  @Nullable
  @jakarta.annotation.Nullable
  public ContentKey maxKey() {
    return maxKey;
  }

  @Nullable
  @jakarta.annotation.Nullable
  public ContentKey prefixKey() {
    return prefixKey;
  }
}
