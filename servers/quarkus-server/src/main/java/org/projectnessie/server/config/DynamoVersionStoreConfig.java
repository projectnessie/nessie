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
package org.projectnessie.server.config;

import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.projectnessie.versioned.dynamodb.DynamoStoreConfig;

import io.quarkus.arc.config.ConfigProperties;
import io.quarkus.runtime.annotations.RegisterForReflection;

/**
 * DynamoDB version store configuration.
 */
@ConfigProperties(prefix = "nessie.version.store.dynamo")
public interface DynamoVersionStoreConfig {

  @RegisterForReflection
  public enum DynamoBillingModeType {
    PROVISIONED,
    PAY_PER_REQUEST
  }

  @ConfigProperty(name = "initialize", defaultValue = "false")
  boolean isDynamoInitialize();

  @ConfigProperty(defaultValue = DynamoStoreConfig.TABLE_PREFIX)
  String getTablePrefix();

  @ConfigProperty(name = "tracing", defaultValue = "true")
  boolean enableTracing();

  @ConfigProperty(name = "create-table.read-capacity-units", defaultValue = DynamoStoreConfig.CREATE_TABLE_READ_CAPACITY_UNITS)
  long createTableReadCapacityUnits();

  @ConfigProperty(name = "create-table.write-capacity-units", defaultValue = DynamoStoreConfig.CREATE_TABLE_WRITE_CAPACITY_UNITS)
  long createTableWriteCapacityUnits();

  /**
   * The AWS billing-mode for tables created by Nessie.
   * <p>Defaults to {@value DynamoStoreConfig#BILLING_MODE}</p>
   */
  @ConfigProperty(name = "create-table.billing-mode", defaultValue = DynamoStoreConfig.BILLING_MODE)
  DynamoBillingModeType billingMode();
}
