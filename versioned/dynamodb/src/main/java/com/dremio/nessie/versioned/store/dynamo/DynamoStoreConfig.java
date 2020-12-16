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
package com.dremio.nessie.versioned.store.dynamo;

import java.net.URI;
import java.util.Optional;

import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import software.amazon.awssdk.regions.Region;

@Immutable
public abstract class DynamoStoreConfig {

  public abstract Optional<URI> getEndpoint();

  @Default
  public String getRefTableName() {
    return "nessie_refs";
  }

  @Default
  public String getL1TableName() {
    return "nessie_l1";
  }

  @Default
  public String getL2TableName() {
    return "nessie_l2";
  }

  @Default
  public String getL3TableName() {
    return "nessie_l3";
  }

  @Default
  public String getKeyListTableName() {
    return "nessie_keylists";
  }

  @Default
  public String getValueTableName() {
    return "nessie_values";
  }

  @Default
  public String getCommitMetaTableName() {
    return "nessie_commitmeta";
  }

  @Default
  public boolean initializeDatabase() {
    return true;
  }


  public abstract Optional<Region> getRegion();

  public static ImmutableDynamoStoreConfig.Builder builder() {
    return ImmutableDynamoStoreConfig.builder();
  }
}
