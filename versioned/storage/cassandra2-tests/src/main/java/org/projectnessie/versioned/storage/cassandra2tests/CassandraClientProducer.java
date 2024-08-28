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
package org.projectnessie.versioned.storage.cassandra2tests;

import static com.datastax.oss.driver.api.core.config.TypedDriverOption.CONNECTION_CONNECT_TIMEOUT;
import static com.datastax.oss.driver.api.core.config.TypedDriverOption.CONNECTION_INIT_QUERY_TIMEOUT;
import static com.datastax.oss.driver.api.core.config.TypedDriverOption.CONNECTION_SET_KEYSPACE_TIMEOUT;
import static com.datastax.oss.driver.api.core.config.TypedDriverOption.CONTROL_CONNECTION_TIMEOUT;
import static com.datastax.oss.driver.api.core.config.TypedDriverOption.HEARTBEAT_TIMEOUT;
import static com.datastax.oss.driver.api.core.config.TypedDriverOption.METADATA_SCHEMA_REQUEST_TIMEOUT;
import static com.datastax.oss.driver.api.core.config.TypedDriverOption.REQUEST_LOG_WARNINGS;
import static com.datastax.oss.driver.api.core.config.TypedDriverOption.REQUEST_TIMEOUT;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.auth.AuthProvider;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.OptionsMap;
import com.datastax.oss.driver.api.core.loadbalancing.LoadBalancingPolicy;
import jakarta.annotation.Nullable;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.List;
import org.immutables.value.Value;

@Value.Immutable
abstract class CassandraClientProducer {

  public static ImmutableCassandraClientProducer.Builder builder() {
    return ImmutableCassandraClientProducer.builder();
  }

  abstract List<InetSocketAddress> contactPoints();

  @Nullable
  abstract String localDc();

  @Nullable
  abstract AuthProvider authProvider();

  @Nullable
  abstract LoadBalancingPolicy loadBalancingPolicy();

  public CqlSession createClient() {
    CqlSessionBuilder client = CqlSession.builder().addContactPoints(contactPoints());

    String localDc = localDc();
    if (localDc != null) {
      client.withLocalDatacenter(localDc);
    }

    AuthProvider auth = authProvider();
    if (auth != null) {
      client.withAuthProvider(auth);
    }

    OptionsMap options = OptionsMap.driverDefaults();

    // Increase some timeouts to avoid flakiness
    Duration timeout = Duration.ofSeconds(15);
    options.put(CONNECTION_CONNECT_TIMEOUT, timeout);
    options.put(CONNECTION_INIT_QUERY_TIMEOUT, timeout);
    options.put(CONNECTION_SET_KEYSPACE_TIMEOUT, timeout);
    options.put(REQUEST_TIMEOUT, timeout);
    options.put(HEARTBEAT_TIMEOUT, timeout);
    options.put(METADATA_SCHEMA_REQUEST_TIMEOUT, timeout);
    options.put(CONTROL_CONNECTION_TIMEOUT, timeout);

    // Disable warnings due to tombstone_warn_threshold
    options.put(REQUEST_LOG_WARNINGS, false);

    return client.withConfigLoader(DriverConfigLoader.fromMap(options)).build();
  }
}
