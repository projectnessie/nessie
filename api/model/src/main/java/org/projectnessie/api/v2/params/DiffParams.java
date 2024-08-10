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

import jakarta.annotation.Nullable;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.QueryParam;
import java.util.List;
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
  @Pattern(regexp = Validation.REF_NAME_PATH_REGEX, message = Validation.REF_NAME_PATH_MESSAGE)
  @Parameter(
      description = REF_PARAMETER_DESCRIPTION,
      examples = {
        @ExampleObject(ref = "ref"),
        @ExampleObject(ref = "refWithHash"),
        @ExampleObject(
            ref = "refWithTimestampMillisSinceEpoch",
            description =
                "The commit 'valid for' the timestamp 1685185847230 in ms since epoch on main"),
        @ExampleObject(
            ref = "refWithTimestampInstant",
            description = "The commit 'valid for' the given ISO-8601 instant on main"),
        @ExampleObject(
            ref = "refWithNthPredecessor",
            description = "The 10th commit from HEAD of main"),
        @ExampleObject(
            ref = "refWithMergeParent",
            description =
                "References the merge-parent of commit 2e1cfa82b035c26cbbbdae632cea070514eb8b773f616aaeaf668e2f0be8f10d on main"),
        @ExampleObject(ref = "refDefault"),
        @ExampleObject(ref = "refDetached"),
      })
  @PathParam("from-ref")
  private String fromRef;

  @NotNull
  @Pattern(regexp = Validation.REF_NAME_PATH_REGEX, message = Validation.REF_NAME_PATH_MESSAGE)
  @Parameter(
      description =
          "Same reference spec as in the 'from-ref' parameter but identifying the other tree for comparison.")
  @PathParam("to-ref")
  private String toRef;

  @Parameter(description = REQUESTED_KEY_PARAMETER_DESCRIPTION)
  @QueryParam("key")
  private List<ContentKey> requestedKeys;

  @Parameter(
      description =
          "A Common Expression Language (CEL) expression. An intro to CEL can be found at https://github.com/google/cel-spec/blob/master/doc/intro.md.\n\n"
              + "Usable variables within the expression are:\n\n"
              + "- 'key' (string, namespace + table name), 'keyElements' (list of strings), 'namespace' (string), 'namespaceElements' (list of strings) and 'name' (string, the \"simple\" table name)")
  @QueryParam("filter")
  private String filter;

  public DiffParams() {}

  @Constructor
  DiffParams(
      @NotNull String fromRef,
      @NotNull String toRef,
      @Nullable Integer maxRecords,
      @Nullable String pageToken,
      @Nullable ContentKey minKey,
      @Nullable ContentKey maxKey,
      @Nullable ContentKey prefixKey,
      @Nullable List<ContentKey> requestedKeys,
      @Nullable String filter) {
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
