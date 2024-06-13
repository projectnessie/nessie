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
package org.projectnessie.quarkus.config;

import io.quarkus.runtime.annotations.StaticInitSafe;
import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithName;
import java.util.Optional;
import org.projectnessie.versioned.storage.jdbc.JdbcBackendBaseConfig;

/**
 * Setting {@code nessie.version.store.type=JDBC} enables transactional/RDBMS as the version store
 * used by the Nessie server.
 *
 * <p>Configuration of the datastore will be done by Quarkus and depends on many factors, such as
 * the actual database to use. The property {@code nessie.version.store.persist.jdbc.datasource}
 * will be used to select one of the built-in datasources; currently supported values are: {@code
 * postgresql} (which activates the PostgresQL driver), {@code mariadb} (which activates the MariaDB
 * driver), and {@code mysql} (which targets MySQL backends, but using the MariaDB driver).
 *
 * <p>For example, to configure a PostgresQL connection, the following configuration should be used:
 *
 * <ul>
 *   <li>{@code nessie.version.store.type=JDBC}
 *   <li>{@code nessie.version.store.persist.jdbc.datasource=postgresql}
 *   <li>{@code quarkus.datasource.postgresql.jdbc.url=jdbc:postgresql://localhost:5432/my_database}
 *   <li>{@code quarkus.datasource.postgresql.username=<your username>}
 *   <li>{@code quarkus.datasource.postgresql.password=<your password>}
 *   <li>Other PostgresQL-specific properties can be set using {@code
 *       quarkus.datasource.postgresql.*}
 * </ul>
 *
 * <p>To connect to a MariaDB database instead, the following configuration should be used:
 *
 * <ul>
 *   <li>{@code nessie.version.store.type=JDBC}
 *   <li>{@code nessie.version.store.persist.jdbc.datasource=mariadb}
 *   <li>{@code quarkus.datasource.mariadb.jdbc.url=jdbc:mariadb://localhost:3306/my_database}
 *   <li>{@code quarkus.datasource.mariadb.username=<your username>}
 *   <li>{@code quarkus.datasource.mariadb.password=<your password>}
 *   <li>Other MariaDB-specific properties can be set using {@code quarkus.datasource.mariadb.*}
 * </ul>
 *
 * <p>To connect to a MySQL database instead, the following configuration should be used:
 *
 * <ul>
 *   <li>{@code nessie.version.store.type=JDBC}
 *   <li>{@code nessie.version.store.persist.jdbc.datasource=mysql}
 *   <li>{@code quarkus.datasource.mysql.jdbc.url=jdbc:mysql://localhost:3306/my_database}
 *   <li>{@code quarkus.datasource.mysql.username=<your username>}
 *   <li>{@code quarkus.datasource.mysql.password=<your password>}
 *   <li>Other MySQL-specific properties can be set using {@code quarkus.datasource.mysql.*}
 * </ul>
 *
 * <p>To connect to an H2 in-memory database, the following configuration should be used (note that
 * H2 is not recommended for production):
 *
 * <ul>
 *   <li>{@code nessie.version.store.type=JDBC}
 *   <li>{@code nessie.version.store.persist.jdbc.datasource=h2}
 * </ul>
 *
 * Note: for MySQL, the MariaDB driver is used, as it is compatible with MySQL. You can use either
 * {@code jdbc:mysql} or {@code jdbc:mariadb} as the URL prefix.
 *
 * <p>A complete set of JDBC configuration options can be found on <a
 * href="https://quarkus.io/guides/datasource">quarkus.io</a>.
 */
@StaticInitSafe
@ConfigMapping(prefix = "nessie.version.store.persist.jdbc")
public interface QuarkusJdbcConfig extends JdbcBackendBaseConfig {

  /**
   * The name of the datasource to use. Must correspond to a configured datasource under {@code
   * quarkus.datasource.<name>}. Supported values are: {@code postgresql} {@code mariadb}, {@code
   * mysql} and {@code h2}. If not provided, the default Quarkus datasource, defined using the
   * {@code quarkus.datasource.*} configuration keys, will be used (the corresponding driver is
   * PostgresQL). Note that it is recommended to define "named" JDBC datasources, see <a
   * href="https://quarkus.io/guides/datasource#jdbc-configuration">Quarkus JDBC config
   * reference</a>.
   */
  @WithName("datasource")
  @Override
  Optional<String> datasourceName();
}
