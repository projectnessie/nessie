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
package org.projectnessie.services.impl;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Properties;
import org.projectnessie.api.ConfigApi;
import org.projectnessie.model.ImmutableNessieConfiguration;
import org.projectnessie.model.NessieConfiguration;
import org.projectnessie.services.config.ServerConfig;

public class ConfigApiImpl implements ConfigApi {

  private static final String version = loadVersion();

  private final ServerConfig config;

  public ConfigApiImpl(ServerConfig config) {
    this.config = config;
  }

  private static String loadVersion() {
    URL url = ConfigApiImpl.class.getResource("version.properties");
    if (url == null) {
      throw new IllegalStateException("Missing version.properties resource");
    }

    try (InputStream in = url.openStream()) {
      Properties properties = new Properties();
      properties.load(in);
      return properties.getProperty("nessie.version");
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public NessieConfiguration getConfig() {
    return ImmutableNessieConfiguration.builder()
        .defaultBranch(this.config.getDefaultBranch())
        .version(version)
        .build();
  }
}
