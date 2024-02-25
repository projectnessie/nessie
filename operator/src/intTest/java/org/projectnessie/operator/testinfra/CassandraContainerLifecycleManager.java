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
package org.projectnessie.operator.testinfra;

import static org.testcontainers.containers.CassandraContainer.CQL_PORT;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager.TestInjector.Annotated;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager.TestInjector.MatchesType;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.testcontainers.containers.CassandraContainer;
import org.testcontainers.containers.wait.strategy.Wait;

public class CassandraContainerLifecycleManager
    extends AbstractContainerLifecycleManager<CassandraContainer<?>> {

  static {
    // the init script is executed with driver 3.x, epoll won't be available
    System.setProperty("com.datastax.driver.FORCE_NIO", "true");
  }

  private static final String JVM_OPTS_TEST =
      "-Dcassandra.skip_wait_for_gossip_to_settle=0 "
          + "-Dcassandra.num_tokens=1 "
          + "-Dcassandra.initial_token=0";

  @Target(ElementType.FIELD)
  @Retention(RetentionPolicy.RUNTIME)
  public @interface CassandraContactPoint {}

  @SuppressWarnings("resource")
  @Override
  protected CassandraContainer<?> createContainer() {
    return new CassandraContainer<>(dockerImage("cassandra").asCompatibleSubstituteFor("cassandra"))
        .withInitScript("org/projectnessie/operator/inttests/fixtures/cassandra/init.cql")
        .withEnv("JVM_OPTS", JVM_OPTS_TEST)
        .waitingFor(Wait.forLogMessage(".*Startup complete.*", 1));
  }

  @Override
  public void inject(TestInjector testInjector) {
    super.inject(testInjector);
    String contactPoint = getInDockerIpAddress() + ":" + CQL_PORT;
    testInjector.injectIntoFields(
        contactPoint,
        new MatchesType(String.class).and(new Annotated(CassandraContactPoint.class)));
  }
}
