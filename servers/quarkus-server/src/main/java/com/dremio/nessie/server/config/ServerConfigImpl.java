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
package com.dremio.nessie.server.config;

import com.dremio.nessie.model.ServerConfig;

import io.quarkus.arc.config.ConfigProperties;

@ConfigProperties(prefix = "nessie.server")
public class ServerConfigImpl implements ServerConfig {

  private String defaultBranch = "main";

  @Override
  public String getDefaultBranch() {
    return defaultBranch;
  }

  public void setDefaultBranch(String defaultTag) {
    this.defaultBranch = defaultTag;
  }
}
