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
package com.dremio.nessie.model;

import java.util.List;

import org.immutables.value.Value;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

@Value.Immutable(prehash = true)
@JsonSerialize(as = ImmutableHiveTable.class)
@JsonDeserialize(as = ImmutableHiveTable.class)
@JsonTypeName("HIVE_TABLE")
public interface HiveTable extends Contents {

  @JsonProperty(index = 1)
  byte[] getTableDefinition();

  @JsonProperty(index = 2)
  List<byte[]> getPartitions();

}
