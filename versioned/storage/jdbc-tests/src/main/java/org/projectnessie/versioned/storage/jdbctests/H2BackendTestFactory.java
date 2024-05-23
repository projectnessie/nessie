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
package org.projectnessie.versioned.storage.jdbctests;

import java.util.Map;
import org.projectnessie.versioned.storage.jdbc.JdbcBackendFactory;

public final class H2BackendTestFactory extends AbstractJdbcBackendTestFactory {

  @Override
  public String getName() {
    return JdbcBackendFactory.NAME + "-H2";
  }

  @Override
  public String jdbcUrl() {
    return "jdbc:h2:mem:nessie;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DEFAULT_NULL_ORDERING=HIGH";
  }

  @Override
  public String jdbcUser() {
    return null;
  }

  @Override
  public String jdbcPass() {
    return null;
  }

  @Override
  public Map<String, String> getQuarkusConfig() {
    return Map.of("quarkus.datasource.postgresql.jdbc.url", jdbcUrl());
  }
}
