/*
 * Copyright (C) 2022 Dremio
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
package org.projectnessie.versioned.storage.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import java.time.Duration;
import org.immutables.value.Value;

@Value.Immutable
public interface CassandraBackendConfig {
  CqlSession client();

  @Value.Default
  default String keyspace() {
    return "nessie";
  }

  /** Timeout used when creating tables. */
  @Value.Default
  default Duration ddlTimeout() {
    return Duration.parse(DEFAULT_DDL_TIMEOUT);
  }

  /** Timeout used for queries and updates. */
  @Value.Default
  default Duration dmlTimeout() {
    return Duration.parse(DEFAULT_DML_TIMEOUT);
  }

  String DEFAULT_DDL_TIMEOUT = "PT5S";

  String DEFAULT_DML_TIMEOUT = "PT3S";

  static ImmutableCassandraBackendConfig.Builder builder() {
    return ImmutableCassandraBackendConfig.builder();
  }
}
