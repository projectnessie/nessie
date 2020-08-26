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

package com.dremio.nessie.serverless;

import java.util.HashMap;

import javax.enterprise.context.Dependent;
import javax.enterprise.inject.Produces;

import com.dremio.nessie.server.ServerConfiguration;
import com.dremio.nessie.services.ServerConfigurationImpl;
import com.dremio.nessie.services.ServerConfigurationImpl.ServerAuthConfigurationImpl;
import com.dremio.nessie.services.ServerConfigurationImpl.ServerDatabaseConfigurationImpl;
import com.dremio.nessie.services.ServerConfigurationImpl.ServiceConfigurationImpl;

@Dependent
public class ConfigurationProducer {


  /**
   * default config for lambda function.
   */
  @Produces
  public ServerConfiguration configuration() {
    return new ServerConfigurationImpl(
      new ServerDatabaseConfigurationImpl("", new HashMap<>()),
      new ServerAuthConfigurationImpl("", false, false, "", ""),
      new ServiceConfigurationImpl(19120, false, false, false, true),
      "master"
    );
  }
}
