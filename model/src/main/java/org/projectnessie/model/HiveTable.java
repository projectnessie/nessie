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
package org.projectnessie.model;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import org.immutables.value.Value;

@Value.Immutable(prehash = true)
@JsonSerialize(as = ImmutableHiveTable.class)
@JsonDeserialize(as = ImmutableHiveTable.class)
@JsonTypeName("HIVE_TABLE")
public abstract class HiveTable extends Contents {

  public abstract byte[] getTableDefinition();

  public abstract List<byte[]> getPartitions();

  /**
   * Because of the List of byte arrays Immutables doesn't generate a correct hashcode.
   *
   * <p>We hand compute the List elements to ensure the byte arrays hash codes are consistent.
   */
  @Override
  public int hashCode() {
    int h = 1;
    h += (31 * h) + Objects.hashCode(getId());
    h += (31 * h) + Arrays.hashCode(getTableDefinition());
    for (byte[] p : getPartitions()) {
      h += (31 * h) + Arrays.hashCode(p);
    }
    return h;
  }

  /**
   * Because of the List of byte arrays Immutables doesn't generate a correct equals.
   *
   * <p>,We hand compare the List elements to ensure the byte arrays are equal.
   */
  @Override
  public boolean equals(Object another) {
    if (this == another) {
      return true;
    }
    return another instanceof ImmutableHiveTable && equalTo((ImmutableHiveTable) another);
  }

  private boolean equalTo(ImmutableHiveTable another) {
    if (hashCode() != another.hashCode()) {
      return false;
    }
    return Objects.equals(getId(), another.getId())
        && Arrays.equals(getTableDefinition(), another.getTableDefinition())
        && partitionsEqual(getPartitions(), another.getPartitions());
  }

  private boolean partitionsEqual(List<byte[]> partitions, List<byte[]> partitions1) {
    if (partitions.size() != partitions1.size()) {
      return false;
    }
    for (int i = 0; i < partitions.size(); i++) {
      if (!Arrays.equals(partitions.get(i), partitions1.get(i))) {
        return false;
      }
    }
    return true;
  }
}
