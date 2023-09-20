/*
 * Copyright (C) 2023 Dremio
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
package org.projectnessie.client.config;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides functionality to retrieve configuration values from various sources, like Java
 * properties, maps and environment variables.
 *
 * <p>Note that the current implementation does not need any additional libraries. If there is a
 * need for advanced features like config encryption or expressions, the implementation might start
 * using SmallRye-Config.
 */
public final class NessieClientConfigSources {

  private static final Logger LOGGER = LoggerFactory.getLogger(NessieClientConfigSources.class);

  private NessieClientConfigSources() {}

  /**
   * Uses values from the {@code ~/.env} file.
   *
   * <p>Similar to the behavior of smallrye-config, the {@code ~/.env} file must be in the Java
   * properties file format.
   *
   * @see #environmentFileConfigSource(Path)
   * @see #propertyNameToEnvironmentName(String)
   */
  public static NessieClientConfigSource dotEnvFileConfigSource() {
    Path dotEnvFile = dotEnvFile();
    return environmentFileConfigSource(dotEnvFile);
  }

  /**
   * Uses values from the {@code ~/.config/nessie/nessie-client.properties} file.
   *
   * @see #systemPropertiesConfigSource()
   * @see #propertiesFileConfigSource(Path)
   * @see #propertiesConfigSource(Properties)
   */
  public static NessieClientConfigSource nessieClientConfigFileConfigSource() {
    Path propertiesFile = nessieClientConfigFile();
    return propertiesFileConfigSource(propertiesFile);
  }

  /**
   * Uses values from the system environment, keys follow the {@link
   * #propertyNameToEnvironmentName(String) environment name mapping}.
   *
   * @see #environmentConfigSource(Map)
   * @see #dotEnvFileConfigSource()
   */
  public static NessieClientConfigSource systemEnvironmentConfigSource() {
    return environmentConfigSource(System.getenv());
  }

  /**
   * Uses values from the Java system properties of the process.
   *
   * @see #propertiesConfigSource(Properties)
   */
  public static NessieClientConfigSource systemPropertiesConfigSource() {
    return propertiesConfigSource(System.getProperties());
  }

  /**
   * Uses values from the given file, where keys in the file follow the {@link
   * #propertyNameToEnvironmentName(String) environment name mapping}.
   *
   * <p>Similar to the behavior of smallrye-config, the {@code .env} file must be in the Java
   * properties file format.
   *
   * @see #dotEnvFileConfigSource()
   * @see #propertyNameToEnvironmentName(String)
   */
  public static NessieClientConfigSource environmentFileConfigSource(Path envFile) {
    if (!Files.isRegularFile(envFile)) {
      return k -> null;
    }
    Properties props = new Properties();
    try (BufferedReader input = Files.newBufferedReader(envFile)) {
      props.load(input);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    return k -> {
      String envName = propertyNameToEnvironmentName(k);
      String v = props.getProperty(envName);
      LOGGER.debug("Config value for key {} as {} retrieved from {}", k, envName, envFile);
      return v;
    };
  }

  /**
   * Uses values from the given map, where keys in the file follow the {@link
   * #propertyNameToEnvironmentName(String) environment name mapping}.
   *
   * @see #systemEnvironmentConfigSource()
   * @see #environmentFileConfigSource(Path)
   * @see #dotEnvFileConfigSource()
   */
  public static NessieClientConfigSource environmentConfigSource(Map<String, String> environment) {
    return k -> {
      String envName = propertyNameToEnvironmentName(k);
      String v = environment.get(envName);
      LOGGER.debug("Config value for key {} as {} retrieved from environment", k, envName);
      return v;
    };
  }

  /**
   * Uses values from the given properties file.
   *
   * @see #propertiesConfigSource(Properties)
   * @see #nessieClientConfigFileConfigSource()
   */
  public static NessieClientConfigSource propertiesFileConfigSource(Path propertiesFile) {
    if (!Files.isRegularFile(propertiesFile)) {
      return k -> null;
    }
    Properties props = new Properties();
    try (BufferedReader input = Files.newBufferedReader(propertiesFile)) {
      props.load(input);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    return k -> {
      String v = props.getProperty(k);
      LOGGER.debug("Config value for key {} retrieved from {}", k, propertiesFile);
      return v;
    };
  }

  /**
   * Uses values from the given {@link Properties}.
   *
   * @see #systemPropertiesConfigSource()
   * @see #mapConfigSource(Map)
   */
  public static NessieClientConfigSource propertiesConfigSource(Properties properties) {
    return k -> {
      String v = properties.getProperty(k);
      LOGGER.debug("Config value for key {} retrieved from properties", k);
      return v;
    };
  }

  /**
   * Uses values from the given {@link Map}.
   *
   * @see #propertiesConfigSource(Properties)
   */
  public static NessieClientConfigSource mapConfigSource(Map<String, String> properties) {
    return k -> {
      String v = properties.get(k);
      LOGGER.debug("Config value for key {} retrieved from map", k);
      return v;
    };
  }

  /**
   * Converts a given property name to the "environment variable name syntax", using upper-case
   * characters and converting {@code .} ("dot") and {@code -} ("minus") to {@code _}
   * ("underscore").
   */
  public static String propertyNameToEnvironmentName(String propertyName) {
    return propertyName.toUpperCase(Locale.ROOT).replace('.', '_').replace('-', '_');
  }

  /**
   * Creates a configuration value retriever using reasonable default config sources.
   *
   * <p>Config values are retrieved from the following sources:
   *
   * <ol>
   *   <li>Java system properties, see {@link #systemPropertiesConfigSource()}
   *   <li>Process environment, see {@link #systemEnvironmentConfigSource()}
   *   <li>{@code ~/.config/nessie/nessie-client-properties} file, see {@link
   *       #nessieClientConfigFileConfigSource()}
   *   <li>{@code ~/.env} file, see {@link #dotEnvFileConfigSource()}
   * </ol>
   */
  public static NessieClientConfigSource defaultConfigSources() {
    return systemPropertiesConfigSource()
        .fallbackTo(systemEnvironmentConfigSource())
        .fallbackTo(nessieClientConfigFileConfigSource())
        .fallbackTo(dotEnvFileConfigSource());
  }

  public static NessieClientConfigSource emptyConfigSource() {
    return k -> null;
  }

  static Path dotEnvFile() {
    return Paths.get(System.getProperty("user.dir"), ".env");
  }

  static Path nessieClientConfigFile() {
    return Paths.get(
        System.getProperty("user.dir"), ".config", "nessie", "nessie-client.properties");
  }
}
