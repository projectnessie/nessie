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
package org.projectnessie.server.configchecks;

import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import org.projectnessie.quarkus.config.QuarkusJdbcConfig;
import org.projectnessie.quarkus.config.QuarkusServerConfig;
import org.projectnessie.quarkus.config.VersionStoreConfig;
import org.projectnessie.server.config.QuarkusNessieAuthenticationConfig;
import org.projectnessie.server.config.QuarkusNessieAuthorizationConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfigChecks {
  private static final Logger LOGGER = LoggerFactory.getLogger(ConfigChecks.class);

  @Inject VersionStoreConfig versionStoreConfig;
  @Inject QuarkusNessieAuthenticationConfig authenticationConfig;
  @Inject QuarkusNessieAuthorizationConfig authorizationConfig;
  @Inject QuarkusServerConfig serverConfig;
  @Inject QuarkusJdbcConfig jdbcConfig;

  public void configCheck(@Observes StartupEvent event) {
    if (versionStoreConfig.getVersionStoreType() == VersionStoreConfig.VersionStoreType.IN_MEMORY) {
      LOGGER.warn(
          "Configured version store type IN_MEMORY is only for testing purposes and experimentation, not for production use. "
              + "Data will be lost when the process is shut down. "
              + "Recommended action: Use a supported database, see https://projectnessie.org/nessie-latest/configuration/");
    }
    if (versionStoreConfig.getVersionStoreType() == VersionStoreConfig.VersionStoreType.JDBC) {
      if (jdbcConfig.datasourceName().isPresent()
          && jdbcConfig.datasourceName().get().equalsIgnoreCase("h2")) {
        LOGGER.warn(
            "Configured datasource H2 is only for testing purposes and experimentation, not for production use. "
                + "Data will be lost when the process is shut down. "
                + "Recommended action: Use a supported production-ready database, see https://projectnessie.org/nessie-latest/configuration/");
      }
    }

    // AuthZ + AuthN warnings
    if (!authorizationConfig.enabled() && !authenticationConfig.enabled()) {
      LOGGER.warn(
          "Both authentication (AuthN) and authorization (AuthZ) are disabled, "
              + "all requests to Nessie will be permitted. "
              + "This means: everybody with access to Nessie can read, write and change everything. "
              + "Recommended action: Enable AuthN & AuthZ, see https://projectnessie.org/nessie-latest/configuration/");
    } else if (!authenticationConfig.enabled()) {
      LOGGER.warn(
          "Authentication (AuthN) is disabled and all requests to Nessie will be permitted. "
              + "This means: everybody with access to Nessie can read, write and change everything. "
              + "Recommended action: Enable authentication, see https://projectnessie.org/nessie-latest/configuration/");
    } else if (!authorizationConfig.enabled()) {
      LOGGER.warn(
          "Authorization (AuthZ) is disabled and all authenticated requests to Nessie will be permitted. "
              + "This means: everybody with access to Nessie can read, write and change everything. "
              + "If you really intent to give every authenticated user access and get rid of this warning, "
              + "explicitly add an allow-all authorization rule (ex: nessie.server.authorization.rules.allow_all=true). "
              + "Recommended action: Enable authorization and configure authorization rules, see https://projectnessie.org/nessie-latest/configuration/");
    }

    if (serverConfig.sendStacktraceToClient()) {
      LOGGER.warn(
          "Java stack traces are sent back in HTTP error responses. "
              + "It is not good practice to send Java stack traces to clients, because "
              + "stack traces might be considered a security risk. "
              + "Recommended action: disable the option, see https://projectnessie.org/nessie-latest/configuration/");
    }
  }
}
