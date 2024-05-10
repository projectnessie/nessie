/*
 * Copyright (C) 2023 Dremio
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
package org.projectnessie.catalog.model.manifest;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.util.UUID;
import javax.annotation.Nullable;
import org.projectnessie.catalog.model.id.Hashable;
import org.projectnessie.catalog.model.id.NessieIdHasher;
import org.projectnessie.nessie.immutables.NessieImmutable;

@NessieImmutable
@JsonSerialize(as = ImmutableNessieFieldValue.class)
@JsonDeserialize(as = ImmutableNessieFieldValue.class)
public interface NessieFieldValue extends Hashable {
  UUID fieldId();

  @Nullable
  @jakarta.annotation.Nullable
  @JsonInclude(JsonInclude.Include.NON_NULL)
  Integer icebergFieldId();

  byte[] value();

  @Override
  default void hash(NessieIdHasher idHasher) {
    idHasher.hash(fieldId());
  }

  static Builder builder() {
    return ImmutableNessieFieldValue.builder();
  }

  @SuppressWarnings("unused")
  interface Builder {
    @CanIgnoreReturnValue
    Builder from(NessieFieldValue instance);

    @CanIgnoreReturnValue
    Builder fieldId(UUID fieldId);

    @CanIgnoreReturnValue
    Builder icebergFieldId(@Nullable Integer icebergFieldId);

    @CanIgnoreReturnValue
    Builder value(byte[] value);

    NessieFieldValue build();
  }
}
