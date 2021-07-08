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

import io.smallrye.config.ConfigMapping;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.projectnessie.versioned.dynamodb.DynamoStoreConfig;

/** DynamoDB version store configuration. */
@ConfigMapping(prefix = "nessie.version.store.dynamo")
public interface DynamoVersionStoreConfig {

  @ConfigProperty(name = "initialize", defaultValue = "false")
  boolean isDynamoInitialize();

  @ConfigProperty(defaultValue = DynamoStoreConfig.TABLE_PREFIX)
  String getTablePrefix();

  @ConfigProperty(name = "tracing", defaultValue = "true")
  boolean enableTracing();
}
