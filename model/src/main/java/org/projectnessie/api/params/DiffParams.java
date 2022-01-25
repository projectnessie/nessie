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

import java.util.Objects;
import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;
import javax.ws.rs.PathParam;
import org.eclipse.microprofile.openapi.annotations.media.ExampleObject;
import org.eclipse.microprofile.openapi.annotations.parameters.Parameter;
import org.projectnessie.model.Validation;

public class DiffParams {

  public static final String HASH_OPTIONAL_REGEX = "(" + Validation.HASH_REGEX + ")?";

  @NotNull
  @Pattern(regexp = Validation.REF_NAME_REGEX, message = Validation.REF_NAME_MESSAGE)
  @Parameter(
      description = "The 'from' reference to start the diff from",
      examples = {@ExampleObject(ref = "ref")})
  @PathParam("fromRef")
  private String fromRef;

  @Nullable
  @Pattern(regexp = HASH_OPTIONAL_REGEX, message = Validation.HASH_MESSAGE)
  @Parameter(
      description = "Optional hash on the 'from' reference to start the diff from",
      examples = {@ExampleObject(ref = "hash")})
  @PathParam("fromHashOnRef")
  private String fromHashOnRef;

  @NotNull
  @Pattern(regexp = Validation.REF_NAME_REGEX, message = Validation.REF_NAME_MESSAGE)
  @Parameter(
      description = "The 'to' reference to end the diff at.",
      examples = {@ExampleObject(ref = "ref")})
  @PathParam("toRef")
  private String toRef;

  @Nullable
  @Pattern(regexp = HASH_OPTIONAL_REGEX, message = Validation.HASH_MESSAGE)
  @Parameter(
      description = "Optional hash on the 'to' reference to end the diff at.",
      examples = {@ExampleObject(ref = "hash")})
  @PathParam("toHashOnRef")
  private String toHashOnRef;

  public DiffParams() {}

  private DiffParams(String fromRef, String fromHashOnRef, String toRef, String toHashOnRef) {
    this.fromRef = fromRef;
    this.fromHashOnRef = fromHashOnRef;
    this.toRef = toRef;
    this.toHashOnRef = toHashOnRef;
  }

  private DiffParams(Builder builder) {
    this(builder.fromRef, builder.fromHashOnRef, builder.toRef, builder.toHashOnRef);
  }

  public String getFromRef() {
    return fromRef;
  }

  public String getFromHashOnRef() {
    return emptyToNull(fromHashOnRef);
  }

  public String getToRef() {
    return toRef;
  }

  public String getToHashOnRef() {
    return emptyToNull(toHashOnRef);
  }

  private static String emptyToNull(String s) {
    if (s == null || s.isEmpty()) {
      return null;
    }
    if (s.charAt(0) == '*') {
      if (s.length() == 1) {
        return null;
      }
      return s.substring(1);
    }
    return s;
  }

  public static DiffParams.Builder builder() {
    return new Builder();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DiffParams that = (DiffParams) o;
    return Objects.equals(fromRef, that.fromRef)
        && Objects.equals(fromHashOnRef, that.fromHashOnRef)
        && Objects.equals(toRef, that.toRef)
        && Objects.equals(toHashOnRef, that.toHashOnRef);
  }

  @Override
  public int hashCode() {
    return Objects.hash(fromRef, fromHashOnRef, toRef, toHashOnRef);
  }

  public static class Builder {
    private String fromRef;
    private String fromHashOnRef;
    private String toRef;
    private String toHashOnRef;

    public Builder() {}

    public Builder from(DiffParams params) {
      return fromRef(params.fromRef)
          .fromHashOnRef(params.fromHashOnRef)
          .toRef(params.toRef)
          .toHashOnRef(params.toHashOnRef);
    }

    public Builder fromRef(String fromRef) {
      this.fromRef = fromRef;
      return this;
    }

    public Builder fromHashOnRef(String fromHashOnRef) {
      this.fromHashOnRef = fromHashOnRef;
      return this;
    }

    public Builder toRef(String toRef) {
      this.toRef = toRef;
      return this;
    }

    public Builder toHashOnRef(String toHashOnRef) {
      this.toHashOnRef = toHashOnRef;
      return this;
    }

    private void validate() {
      Objects.requireNonNull(fromRef, "fromRef must be non-null");
      Objects.requireNonNull(toRef, "toRef must be non-null");
    }

    public DiffParams build() {
      validate();
      return new DiffParams(this);
    }
  }
}
