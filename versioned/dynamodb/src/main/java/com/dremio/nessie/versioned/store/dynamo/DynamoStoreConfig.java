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

  // DEFAULT TABLE NAMES
  public static final String REF_TABLE = "nessie_refs";
  public static final String L1_TABLE = "nessie_l1";
  public static final String L2_TABLE = "nessie_l2";
  public static final String L3_TABLE = "nessie_l3";
  public static final String KEY_LIST_TABLE = "nessie_keylists";
  public static final String VALUE_TABLE = "nessie_values";
  public static final String COMMIT_META_TABLE = "nessie_commitmeta";

  public abstract Optional<URI> getEndpoint();

  @Default
  public String getRefTableName() {
    return REF_TABLE;
  }

  @Default
  public String getL1TableName() {
    return L1_TABLE;
  }

  @Default
  public String getL2TableName() {
    return L2_TABLE;
  }

  @Default
  public String getL3TableName() {
    return L3_TABLE;
  }

  @Default
  public String getKeyListTableName() {
    return KEY_LIST_TABLE;
  }

  @Default
  public String getValueTableName() {
    return VALUE_TABLE;
  }

  @Default
  public String getCommitMetaTableName() {
    return COMMIT_META_TABLE;
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
