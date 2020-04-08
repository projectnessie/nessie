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
package com.dremio.iceberg.server;

import java.io.IOException;
import java.net.URL;

import org.glassfish.hk2.api.Factory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

public class ConfigurationImpl implements Configuration {
  private static final Logger logger = LoggerFactory.getLogger(ConfigurationImpl.class);
  private final String dbClassName;

  public ConfigurationImpl() {
    dbClassName = "com.dremio.iceberg.backend.simple.InMemory";
  }

  @Override
  public String getDbClassName() {
    return dbClassName;
  }

  public static class ConfigurationFactory implements Factory<Configuration> {
    private final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
    @Override
    public Configuration provide() {
      try {
        URL config = getClass().getClassLoader().getResource("config.yaml");
        return mapper.readValue(config, ConfigurationImpl.class);
      } catch (IOException | NullPointerException e) {
        logger.error("Unable to read config, continuing with defaults", e);
        return new ConfigurationImpl();
      }
    }

    @Override
    public void dispose(Configuration configuration) {

    }
  }
}
