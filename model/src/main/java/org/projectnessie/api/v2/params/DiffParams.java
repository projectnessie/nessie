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

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;
import javax.ws.rs.PathParam;
import org.eclipse.microprofile.openapi.annotations.media.ExampleObject;
import org.eclipse.microprofile.openapi.annotations.parameters.Parameter;
import org.immutables.builder.Builder.Constructor;
import org.projectnessie.model.Validation;

/**
 * Parameters for the {@code getDiff} API method.
 *
 * <p>{@code startKey} plus {@code endKey} selectors and pagination will be added soon. The
 * intention is to allow clients to request a diff on a sub-set of keys (e.g. in one particular
 * namespace) or just for one particular key.
 */
public class DiffParams {

  @NotNull
  @Pattern(regexp = Validation.REF_NAME_PATH_REGEX, message = Validation.REF_NAME_PATH_MESSAGE)
  @Parameter(
      description = REF_PARAMETER_DESCRIPTION,
      examples = {
        @ExampleObject(ref = "ref"),
        @ExampleObject(ref = "refWithHash"),
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

  public DiffParams() {}

  @Constructor
  DiffParams(@NotNull String fromRef, @NotNull String toRef) {
    this.fromRef = fromRef;
    this.toRef = toRef;
  }

  public String getFromRef() {
    return fromRef;
  }

  public String getToRef() {
    return toRef;
  }

  public static DiffParamsBuilder builder() {
    return new DiffParamsBuilder();
  }
}
