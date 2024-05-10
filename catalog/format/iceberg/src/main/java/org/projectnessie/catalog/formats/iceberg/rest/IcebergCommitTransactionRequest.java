/*
 * Copyright (C) 2024 Dremio
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
package org.projectnessie.catalog.formats.iceberg.rest;

import static com.google.common.base.Preconditions.checkArgument;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonNaming;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.List;
import org.immutables.value.Value;
import org.projectnessie.nessie.immutables.NessieImmutable;

@NessieImmutable
@JsonSerialize(as = ImmutableIcebergCommitTransactionRequest.class)
@JsonDeserialize(as = ImmutableIcebergCommitTransactionRequest.class)
@JsonNaming(PropertyNamingStrategies.KebabCaseStrategy.class)
@JsonIgnoreProperties(ignoreUnknown = true)
public interface IcebergCommitTransactionRequest {
  List<IcebergUpdateTableRequest> tableChanges();

  @Value.Check
  default void check() {
    checkArgument(!tableChanges().isEmpty(), "Invalid table changes: empty");
    for (IcebergUpdateTableRequest tableChange : tableChanges()) {
      checkArgument(
          null != tableChange.identifier(), "Invalid table changes: table identifier is required");
    }
  }

  static Builder builder() {
    return ImmutableIcebergCommitTransactionRequest.builder();
  }

  @SuppressWarnings("unused")
  interface Builder {
    IcebergCommitTransactionRequest build();
  }
}
