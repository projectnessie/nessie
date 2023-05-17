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

import static org.projectnessie.api.v2.doc.ApiDoc.REF_PARAMETER_DESCRIPTION;
import static org.projectnessie.api.v2.doc.ApiDoc.REQUESTED_KEY_PARAMETER_DESCRIPTION;

import java.util.List;
import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import org.eclipse.microprofile.openapi.annotations.media.ExampleObject;
import org.eclipse.microprofile.openapi.annotations.parameters.Parameter;
import org.immutables.builder.Builder.Constructor;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.Validation;

/**
 * Parameters for the {@code getDiff} API method.
 *
 * <p>{@code startKey} plus {@code endKey} selectors and pagination will be added soon. The
 * intention is to allow clients to request a diff on a sub-set of keys (e.g. in one particular
 * namespace) or just for one particular key.
 */
public class DiffParams extends KeyRangeParams<DiffParams> {

  @NotNull
  @jakarta.validation.constraints.NotNull
  @Pattern(regexp = Validation.REF_NAME_PATH_REGEX, message = Validation.REF_NAME_PATH_MESSAGE)
  @jakarta.validation.constraints.Pattern(
      regexp = Validation.REF_NAME_PATH_REGEX,
      message = Validation.REF_NAME_PATH_MESSAGE)
  @Parameter(
      description = REF_PARAMETER_DESCRIPTION,
      examples = {
        @ExampleObject(ref = "ref"),
        @ExampleObject(ref = "refWithHash"),
        @ExampleObject(ref = "refDefault"),
        @ExampleObject(ref = "refDetached"),
      })
  @PathParam("from-ref")
  @jakarta.ws.rs.PathParam("from-ref")
  private String fromRef;

  @NotNull
  @jakarta.validation.constraints.NotNull
  @Pattern(regexp = Validation.REF_NAME_PATH_REGEX, message = Validation.REF_NAME_PATH_MESSAGE)
  @jakarta.validation.constraints.Pattern(
      regexp = Validation.REF_NAME_PATH_REGEX,
      message = Validation.REF_NAME_PATH_MESSAGE)
  @Parameter(
      description =
          "Same reference spec as in the 'from-ref' parameter but identifying the other tree for comparison.")
  @PathParam("to-ref")
  @jakarta.ws.rs.PathParam("to-ref")
  private String toRef;

  @Parameter(description = REQUESTED_KEY_PARAMETER_DESCRIPTION)
  @QueryParam("key")
  @jakarta.ws.rs.QueryParam("key")
  private List<ContentKey> requestedKeys;

  @Parameter(
      description =
          "A Common Expression Language (CEL) expression. An intro to CEL can be found at https://github.com/google/cel-spec/blob/master/doc/intro.md.\n\n"
              + "Usable variables within the expression are:\n\n"
              + "- 'key' (string, namespace + table name), 'keyElements' (list of strings), 'namespace' (string), 'namespaceElements' (list of strings) and 'name' (string, the \"simple\" table name)")
  @QueryParam("filter")
  @jakarta.ws.rs.QueryParam("filter")
  private String filter;

  public DiffParams() {}

  @Constructor
  DiffParams(
      @NotNull @jakarta.validation.constraints.NotNull String fromRef,
      @NotNull @jakarta.validation.constraints.NotNull String toRef,
      @Nullable @jakarta.annotation.Nullable Integer maxRecords,
      @Nullable @jakarta.annotation.Nullable String pageToken,
      @Nullable @jakarta.annotation.Nullable ContentKey minKey,
      @Nullable @jakarta.annotation.Nullable ContentKey maxKey,
      @Nullable @jakarta.annotation.Nullable ContentKey prefixKey,
      @Nullable @jakarta.annotation.Nullable List<ContentKey> requestedKeys,
      @Nullable @jakarta.annotation.Nullable String filter) {
    super(maxRecords, pageToken, minKey, maxKey, prefixKey);
    this.fromRef = fromRef;
    this.toRef = toRef;
    this.requestedKeys = requestedKeys;
    this.filter = filter;
  }

  public String getFromRef() {
    return fromRef;
  }

  public String getToRef() {
    return toRef;
  }

  public List<ContentKey> getRequestedKeys() {
    return requestedKeys;
  }

  public String getFilter() {
    return filter;
  }

  public static DiffParamsBuilder builder() {
    return new DiffParamsBuilder();
  }

  @Override
  public DiffParams forNextPage(String pageToken) {
    return new DiffParams(
        fromRef,
        toRef,
        maxRecords(),
        pageToken,
        minKey(),
        maxKey(),
        prefixKey(),
        requestedKeys,
        filter);
  }
}
