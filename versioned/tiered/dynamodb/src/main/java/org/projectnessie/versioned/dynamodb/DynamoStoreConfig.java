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
package org.projectnessie.versioned.dynamodb;

import java.net.URI;
import java.util.Optional;

import org.immutables.value.Value.Default;
import org.immutables.value.Value.Immutable;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;

@Immutable
public abstract class DynamoStoreConfig {

  // DEFAULT TABLE PREFIXNAMES
  public static final String TABLE_PREFIX = "nessie_";
  public static final String CREATE_TABLE_READ_CAPACITY_UNITS = "50";
  public static final String CREATE_TABLE_WRITE_CAPACITY_UNITS = "50";
  public static final String BILLING_MODE = "PAY_PER_REQUEST";

  public abstract Optional<URI> getEndpoint();

  @Default
  public String getTablePrefix() {
    return TABLE_PREFIX;
  }

  @Default
  public boolean initializeDatabase() {
    return true;
  }

  @Default
  public long getCreateTableReadCapacityUnits() {
    return Long.parseLong(CREATE_TABLE_READ_CAPACITY_UNITS);
  }

  @Default
  public long getCreateTableWriteCapacityUnits() {
    return Long.parseLong(CREATE_TABLE_WRITE_CAPACITY_UNITS);
  }

  @Default
  public BillingMode getBillingMode() {
    return BillingMode.valueOf(BILLING_MODE);
  }

  public abstract Optional<Region> getRegion();

  public static ImmutableDynamoStoreConfig.Builder builder() {
    return ImmutableDynamoStoreConfig.builder();
  }

  @Default
  public boolean enableTracing() {
    return true;
  }
}
