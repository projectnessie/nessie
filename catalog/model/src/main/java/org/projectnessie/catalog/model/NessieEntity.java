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
package org.projectnessie.catalog.model;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeId;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.time.Instant;
import javax.annotation.Nullable;
import org.projectnessie.catalog.model.snapshot.TableFormat;
import org.projectnessie.model.CommitMeta.InstantDeserializer;
import org.projectnessie.model.CommitMeta.InstantSerializer;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({
  @JsonSubTypes.Type(value = NessieTable.class, name = "TABLE"),
  @JsonSubTypes.Type(value = NessieView.class, name = "VIEW")
})
public interface NessieEntity {
  @JsonTypeId
  String type();

  String nessieContentId();

  @Nullable
  @jakarta.annotation.Nullable
  String icebergUuid();

  @Nullable
  @jakarta.annotation.Nullable
  TableFormat tableFormat();

  // FIXME move this serializer out of CommitMeta - or add module
  // com.fasterxml.jackson.datatype:jackson-datatype-jsr310
  @JsonSerialize(using = InstantSerializer.class)
  @JsonDeserialize(using = InstantDeserializer.class)
  Instant createdTimestamp();

  @SuppressWarnings("unused")
  interface Builder<B extends Builder<B>> {
    @CanIgnoreReturnValue
    B nessieContentId(String nessieContentId);

    @CanIgnoreReturnValue
    B icebergUuid(String icebergUuid);

    @CanIgnoreReturnValue
    B tableFormat(TableFormat tableFormat);

    @CanIgnoreReturnValue
    B createdTimestamp(Instant createdTimestamp);
  }
}
