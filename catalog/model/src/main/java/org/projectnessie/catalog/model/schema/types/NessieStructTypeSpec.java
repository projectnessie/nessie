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
package org.projectnessie.catalog.model.schema.types;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.List;
import org.projectnessie.catalog.model.id.NessieIdHasher;
import org.projectnessie.catalog.model.schema.NessieField;
import org.projectnessie.catalog.model.schema.NessieStruct;
import org.projectnessie.nessie.immutables.NessieImmutable;

@NessieImmutable
@JsonSerialize(as = ImmutableNessieStructTypeSpec.class)
@JsonDeserialize(as = ImmutableNessieStructTypeSpec.class)
public interface NessieStructTypeSpec extends NessieTypeSpec {

  NessieStruct struct();

  @Override
  default NessieType type() {
    return NessieType.STRUCT;
  }

  default StringBuilder asString(StringBuilder targetBuffer) {
    targetBuffer.append("struct<");
    List<NessieField> fields = struct().fields();
    for (int i = 0; i < fields.size(); i++) {
      if (i > 0) {
        targetBuffer.append(",");
      }
      fields.get(i).type().asString(targetBuffer);
    }
    return targetBuffer.append(">");
  }

  @Override
  default void hash(NessieIdHasher idHasher) {
    idHasher.hash(type().lowerCaseName()).hash(struct());
  }
}
