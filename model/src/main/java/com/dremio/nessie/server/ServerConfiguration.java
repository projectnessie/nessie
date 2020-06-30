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

package com.dremio.nessie.server;

import java.util.Map;

/**
 * configuration object mapped to yaml/json config document.
 */
public interface ServerConfiguration {

  ServerDatabaseConfiguration getDatabaseConfiguration();

  ServerAuthenticationConfiguration getAuthenticationConfiguration();

  ServiceConfiguration getServiceConfiguration();

  String getDefaultTag();

  /**
   * specific configuration to database backend.
   */
  interface ServerDatabaseConfiguration {

    String getDbClassName();

    Map<String, String> getDbProps();
  }

  /**
   * specific configuration related to authentication backend.
   */
  interface ServerAuthenticationConfiguration {

    String getUserServiceClassName();

    boolean getEnableLoginEndpoint();

    boolean getEnableUsersEndpoint();

    String getAuthFilterClassName();
  }

  /**
   * config related to how/what and where the webserver runs.
   */
  interface ServiceConfiguration {

    int getPort();

    boolean getUi();

    boolean getSwagger();

    boolean getMetrics();
  }
}
