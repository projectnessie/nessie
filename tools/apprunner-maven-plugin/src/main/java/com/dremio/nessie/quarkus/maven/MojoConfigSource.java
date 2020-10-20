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
package com.dremio.nessie.quarkus.maven;

import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

import org.eclipse.microprofile.config.spi.ConfigSource;

/**
 * A config source to override default application when using apprunner.
 */
public class MojoConfigSource implements ConfigSource {
  private static volatile Properties properties = new Properties();

  public static void setProperties(Properties properties) {
    MojoConfigSource.properties = properties;
  }

  @Override
  public Map<String, String> getProperties() {
    Map<String, String> map = new TreeMap<>();
    properties.forEach((k, v) -> map.put(k.toString(), v.toString()));
    return map;
  }

  @Override
  public String getValue(String propertyName) {
    return properties.getProperty(propertyName);
  }

  @Override
  public String getName() {
    return "App Runner";
  }

  @Override
  public int getOrdinal() {
    return Integer.MAX_VALUE;
  }

}
