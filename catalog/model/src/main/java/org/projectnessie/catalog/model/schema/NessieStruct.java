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
package org.projectnessie.catalog.model.schema;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.util.List;
import javax.annotation.Nullable;
import org.projectnessie.catalog.model.id.Hashable;
import org.projectnessie.catalog.model.id.NessieIdHasher;
import org.projectnessie.nessie.immutables.NessieImmutable;

@NessieImmutable
@JsonSerialize(as = ImmutableNessieStruct.class)
@JsonDeserialize(as = ImmutableNessieStruct.class)
public interface NessieStruct extends Hashable {
  static NessieStruct nessieStruct(List<NessieField> fields, String icebergRecordName) {
    return ImmutableNessieStruct.of(fields, icebergRecordName);
  }

  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  List<NessieField> fields();

  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  @Nullable
  @jakarta.annotation.Nullable
  String icebergRecordName();

  @Override
  default void hash(NessieIdHasher idHasher) {
    fields().forEach(f -> idHasher.hash(f.id()));
    idHasher.hash(icebergRecordName());
  }

  static Builder builder() {
    return ImmutableNessieStruct.builder();
  }

  @SuppressWarnings("unused")
  interface Builder {
    @CanIgnoreReturnValue
    Builder addField(NessieField field);

    @CanIgnoreReturnValue
    Builder icebergRecordName(String icebergRecordName);

    NessieStruct build();
  }
}
