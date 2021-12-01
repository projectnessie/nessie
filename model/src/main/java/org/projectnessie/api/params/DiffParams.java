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
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;
import javax.ws.rs.PathParam;
import org.eclipse.microprofile.openapi.annotations.media.ExampleObject;
import org.eclipse.microprofile.openapi.annotations.parameters.Parameter;
import org.projectnessie.model.Validation;

public class DiffParams {

  @NotNull
  @Pattern(regexp = Validation.REF_NAME_REGEX, message = Validation.REF_NAME_MESSAGE)
  @Parameter(
      description = "The 'from' reference to start the diff from",
      examples = {@ExampleObject(ref = "ref")})
  @PathParam("fromRef")
  private String fromRef;

  @NotNull
  @Pattern(regexp = Validation.REF_NAME_REGEX, message = Validation.REF_NAME_MESSAGE)
  @Parameter(
      description = "The 'to' reference to end the diff at.",
      examples = {@ExampleObject(ref = "ref")})
  @PathParam("toRef")
  private String toRef;

  public DiffParams() {}

  private DiffParams(String fromRef, String toRef) {
    this.fromRef = fromRef;
    this.toRef = toRef;
  }

  private DiffParams(Builder builder) {
    this(builder.fromRef, builder.toRef);
  }

  public String getFromRef() {
    return fromRef;
  }

  public String getToRef() {
    return toRef;
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
    return Objects.equals(fromRef, that.fromRef) && Objects.equals(toRef, that.toRef);
  }

  @Override
  public int hashCode() {
    return Objects.hash(fromRef, toRef);
  }

  public static class Builder {
    private String fromRef;
    private String toRef;

    public Builder() {}

    public Builder from(DiffParams params) {
      return fromRef(params.fromRef).toRef(params.toRef);
    }

    public Builder fromRef(String fromRef) {
      this.fromRef = fromRef;
      return this;
    }

    public Builder toRef(String toRef) {
      this.toRef = toRef;
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
