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
package org.projectnessie.model.iceberg;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.List;
import javax.annotation.Nullable;
import org.eclipse.microprofile.openapi.annotations.enums.SchemaType;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.immutables.value.Value;

@Value.Immutable
@JsonSerialize(as = ImmutableIcebergSortOrder.class)
@JsonDeserialize(as = ImmutableIcebergSortOrder.class)
@Schema(type = SchemaType.OBJECT, title = "Iceberg sort order")
public interface IcebergSortOrder {
  int getSchemaId();

  int getOrderId();

  @Nullable
  List<SortField> getFields();

  static ImmutableIcebergSortOrder.Builder builder() {
    return ImmutableIcebergSortOrder.builder();
  }

  @Value.Immutable
  @JsonSerialize(as = ImmutableSortField.class)
  @JsonDeserialize(as = ImmutableSortField.class)
  @Schema(type = SchemaType.OBJECT, title = "Iceberg sort field")
  interface SortField {
    int getSourceId();

    SortDirection getDirection();

    NullOrder getNullOrder();

    String getTransform();

    static ImmutableSortField.Builder builder() {
      return ImmutableSortField.builder();
    }

    enum SortDirection {
      ASC,
      DESC
    }

    enum NullOrder {
      NULLS_FIRST,
      NULLS_LAST
    }
  }
}
