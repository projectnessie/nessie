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
package org.projectnessie.versioned.storage.jdbc;

import java.util.Optional;

public interface JdbcBackendBaseConfig {

  Optional<String> datasourceName();

  /**
   * The JDBC catalog name.
   *
   * @deprecated This setting has never worked as expected and is now ineffective. The catalog must
   *     be specified directly in the JDBC URL using the option {@code
   *     quarkus.datasource.*.jdbc.url}.
   */
  @Deprecated(forRemoval = true)
  Optional<String> catalog();

  /**
   * The JDBC schema name.
   *
   * @deprecated This setting has never worked as expected and is now ineffective. The schema must
   *     be specified directly in the JDBC URL using the option {@code
   *     quarkus.datasource.*.jdbc.url}.
   */
  @Deprecated(forRemoval = true)
  Optional<String> schema();
}
