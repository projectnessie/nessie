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
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.auth.AuthProvider;
import com.datastax.oss.driver.api.core.loadbalancing.LoadBalancingPolicy;
import jakarta.annotation.Nullable;
import java.net.InetSocketAddress;
import java.util.List;
import org.immutables.value.Value;

@Value.Immutable
public abstract class CassandraClientProducer {

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
    CqlSessionBuilder cluster = CqlSession.builder().addContactPoints(contactPoints());

    String localDc = localDc();
    if (localDc != null) {
      cluster.withLocalDatacenter(localDc);
    }

    AuthProvider auth = authProvider();
    if (auth != null) {
      cluster.withAuthProvider(auth);
    }

    return cluster.build();
  }
}
