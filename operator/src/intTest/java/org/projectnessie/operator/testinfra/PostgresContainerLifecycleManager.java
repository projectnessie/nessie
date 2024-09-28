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

import static org.testcontainers.containers.PostgreSQLContainer.POSTGRESQL_PORT;

import io.quarkus.test.common.QuarkusTestResourceLifecycleManager.TestInjector.Annotated;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager.TestInjector.MatchesType;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.testcontainers.containers.PostgreSQLContainer;

public class PostgresContainerLifecycleManager
    extends AbstractContainerLifecycleManager<PostgreSQLContainer<?>> {

  @Target(ElementType.FIELD)
  @Retention(RetentionPolicy.RUNTIME)
  public @interface JdbcUrl {}

  @SuppressWarnings("resource")
  @Override
  protected PostgreSQLContainer<?> createContainer() {
    return new PostgreSQLContainer<>(dockerImage("postgres").asCompatibleSubstituteFor("postgres"))
        .withDatabaseName("nessie")
        .withUsername("nessie")
        .withPassword("nessie");
  }

  @Override
  public void inject(TestInjector testInjector) {
    super.inject(testInjector);
    String jdbcUrl =
        "jdbc:postgresql://%s:%d/nessie".formatted(getInDockerIpAddress(), POSTGRESQL_PORT);
    testInjector.injectIntoFields(
        jdbcUrl, new MatchesType(String.class).and(new Annotated(JdbcUrl.class)));
  }
}
