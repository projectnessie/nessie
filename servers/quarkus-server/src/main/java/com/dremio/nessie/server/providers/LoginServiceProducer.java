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
package com.dremio.nessie.server.providers;

import javax.enterprise.inject.Produces;
import javax.inject.Inject;

import com.dremio.nessie.auth.LoginService;
import com.dremio.nessie.server.config.ApplicationConfig.ServerConfigImpl;
import com.dremio.nessie.server.config.converters.LoginType;
import com.dremio.nessie.services.auth.BasicUserService;
import com.dremio.nessie.services.auth.NoopLoginService;

public class LoginServiceProducer {

  private final ServerConfigImpl config;

  @Inject
  public LoginServiceProducer(ServerConfigImpl config) {
    this.config = config;
  }

  /**
   * produces a service to generate JWTs.
   */
  @Produces
  public LoginService loginService() {
    if (config.getLoginType().equals(LoginType.BASIC)) {
      return new BasicUserService();
    } else if (config.getLoginType().equals(LoginType.NOOP)) {
      return new NoopLoginService();
    } else {
      throw new UnsupportedOperationException(String.format("Unknown login service type %s", config.getLoginType()));
    }
  }
}
