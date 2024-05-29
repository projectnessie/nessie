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

import static java.lang.Boolean.parseBoolean;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_CLIENT_API_VERSION;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_AUTH_ENDPOINT;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_CLIENT_ID;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_CLIENT_SCOPES;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_CLIENT_SECRET;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_OAUTH2_DEFAULT_ACCESS_TOKEN_LIFESPAN;
import static org.projectnessie.client.NessieConfigConstants.CONF_NESSIE_URI;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.projectnessie.client.NessieConfigConstants;
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
  // Keep the anonymous classes, do not use lambdas, because Quarkus tests use xstream for deep
  // cloning, which does not work with lambdas.

  private static final Logger LOGGER = LoggerFactory.getLogger(NessieClientConfigSources.class);
  private static final NessieClientConfigSource EMPTY_CONFIG_SOURCE =
      new NessieClientConfigSource() {
        @Nullable
        @jakarta.annotation.Nullable
        @Override
        public String getValue(@Nonnull @jakarta.annotation.Nonnull String key) {
          return null;
        }
      };

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
      return EMPTY_CONFIG_SOURCE;
    }
    Properties props = loadProperties(envFile);

    return new NessieClientConfigSource() {
      @Nullable
      @jakarta.annotation.Nullable
      @Override
      public String getValue(@Nonnull @jakarta.annotation.Nonnull String key) {
        String envName = propertyNameToEnvironmentName(key);
        String v = props.getProperty(envName);
        LOGGER.debug("Config value for key {} as {} retrieved from {}", key, envName, envFile);
        return v;
      }
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
    return new NessieClientConfigSource() {
      @Nullable
      @jakarta.annotation.Nullable
      @Override
      public String getValue(@Nonnull @jakarta.annotation.Nonnull String key) {
        String envName = propertyNameToEnvironmentName(key);
        String v = environment.get(envName);
        LOGGER.debug("Config value for key {} as {} retrieved from environment", key, envName);
        return v;
      }
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
      return EMPTY_CONFIG_SOURCE;
    }
    Properties props = loadProperties(propertiesFile);

    return new NessieClientConfigSource() {
      @Nullable
      @jakarta.annotation.Nullable
      @Override
      public String getValue(@Nonnull @jakarta.annotation.Nonnull String key) {
        String v = props.getProperty(key);
        LOGGER.debug("Config value for key {} retrieved from {}", key, propertiesFile);
        return v;
      }
    };
  }

  /**
   * Uses values from the given {@link Properties}.
   *
   * @see #systemPropertiesConfigSource()
   * @see #mapConfigSource(Map)
   */
  public static NessieClientConfigSource propertiesConfigSource(Properties properties) {
    return new NessieClientConfigSource() {
      @Nullable
      @jakarta.annotation.Nullable
      @Override
      public String getValue(@Nonnull @jakarta.annotation.Nonnull String key) {
        String v = properties.getProperty(key);
        LOGGER.debug("Config value for key {} retrieved from properties", key);
        return v;
      }
    };
  }

  /**
   * Uses values from the given {@link Map}.
   *
   * @see #propertiesConfigSource(Properties)
   */
  public static NessieClientConfigSource mapConfigSource(Map<String, String> properties) {
    return new NessieClientConfigSource() {
      @Nullable
      @jakarta.annotation.Nullable
      @Override
      public String getValue(@Nonnull @jakarta.annotation.Nonnull String key) {
        String v = properties.get(key);
        LOGGER.debug("Config value for key {} retrieved from map", key);
        return v;
      }
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

  /**
   * Produces a config source from the Iceberg {@code RESTCatalog} properties, mapping Nessie client
   * configs to Iceberg REST catalog properties, requiring REST catalog properties as returned from
   * Nessie Catalog.
   */
  public static NessieClientConfigSource fromIcebergRestCatalogProperties(
      Map<String, String> catalogProperties) {
    if (!parseBoolean(catalogProperties.get("nessie.is-nessie-catalog"))) {
      throw new IllegalArgumentException(
          "The specified properties do not belong to an Iceberg RESTCatalog backed by a Nessie Catalog service.");
    }

    // See o.a.i.rest.auth.OAuth2Properties.CREDENTIAL
    Credential credential = resolveCredential(catalogProperties);

    return x -> {
      switch (x) {
        case CONF_NESSIE_URI:
          // Use the Nessie Core REST API URL provided by Nessie Catalog Server. The Nessie
          // Catalog
          // Server provides a _base_ URI without the `v1` or `v2` suffixes. We can safely
          // assume
          // that `nessie.core-base-uri` contains a `/` terminated URI.
          return catalogProperties.get("nessie.core-base-uri") + "v2";
        case CONF_NESSIE_CLIENT_API_VERSION:
          return "2";
        case CONF_NESSIE_OAUTH2_AUTH_ENDPOINT:
          return catalogProperties.get("oauth2-server-uri");
        case CONF_NESSIE_OAUTH2_CLIENT_ID:
          return credential.clientId;
        case CONF_NESSIE_OAUTH2_CLIENT_SECRET:
          // See o.a.i.rest.auth.OAuth2Properties.CREDENTIAL
          return credential.secret;
        case CONF_NESSIE_OAUTH2_CLIENT_SCOPES:
          // Same default scope as the Iceberg REST Client uses in
          // o.a.i.rest.RESTSessionCatalog.initialize
          // See o.a.i.rest.auth.OAuth2Util.SCOPE
          return resolveOAuthScope(catalogProperties);
        case CONF_NESSIE_OAUTH2_DEFAULT_ACCESS_TOKEN_LIFESPAN:
          return resolveTokenLifespan(catalogProperties);
        default:
          // TODO need the "token" (initial bearer token for OAuth2 as in
          //  o.a.i.rest.RESTSessionCatalog.initialize?
          // TODO Nessie has way more OAuth configurations than Iceberg. What shall we do there?
          String v = catalogProperties.get(x);
          return v != null ? v : catalogProperties.get(x.replace("nessie.", ""));
      }
    };
  }

  static String resolveTokenLifespan(Map<String, String> catalogProperties) {
    String life = catalogProperties.getOrDefault("token-expires-in-ms", "3600000");
    return Duration.ofMillis(Long.parseLong(life)).toString();
  }

  /**
   * Collects the OAuth scopes from both {@value
   * NessieConfigConstants#CONF_NESSIE_OAUTH2_CLIENT_SCOPES} and {@code scope} properties, always
   * adds the {@code catalog} scope.
   */
  static String resolveOAuthScope(Map<String, String> catalogProperties) {
    String nessieScope =
        "catalog "
            + resolveViaEnvironment(catalogProperties, CONF_NESSIE_OAUTH2_CLIENT_SCOPES, "")
            + " "
            + resolveViaEnvironment(catalogProperties, "scope", "");
    return Arrays.stream(nessieScope.split(" "))
        .map(String::trim)
        .filter(s -> !s.isEmpty())
        .distinct()
        .collect(Collectors.joining(" "));
  }

  static Credential resolveCredential(Map<String, String> catalogProperties) {
    String nessieClientId =
        resolveViaEnvironment(catalogProperties, CONF_NESSIE_OAUTH2_CLIENT_ID, null);
    String nessieClientSecret =
        resolveViaEnvironment(catalogProperties, CONF_NESSIE_OAUTH2_CLIENT_SECRET, null);

    Credential credentialFromIceberg =
        parseIcebergCredential(resolveViaEnvironment(catalogProperties, "credential", null));

    return new Credential(
        nessieClientId != null ? nessieClientId : credentialFromIceberg.clientId,
        nessieClientSecret != null ? nessieClientSecret : credentialFromIceberg.secret);
  }

  /** Allow resolving a property via the environment. */
  static String resolveViaEnvironment(
      Map<String, String> properties, String property, String defaultValue) {
    String value = properties.get(property);
    if (value == null) {
      return defaultValue;
    }
    if (value.startsWith("env:")) {
      String env = System.getenv(value.substring("env:".length()));
      if (env == null) {
        return defaultValue;
      }
      return env;
    } else {
      return value;
    }
  }

  static Credential parseIcebergCredential(String credential) {
    // See o.a.i.rest.auth.OAuth2Util.parseCredential
    if (credential == null) {
      return new Credential(null, null);
    }
    int colon = credential.indexOf(':');
    if (colon == -1) {
      return new Credential(null, credential);
    } else {
      return new Credential(credential.substring(0, colon), credential.substring(colon + 1));
    }
  }

  static final class Credential {
    final String clientId;
    final String secret;

    Credential(String clientId, String secret) {
      this.clientId = clientId;
      this.secret = secret;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      Credential that = (Credential) o;
      return Objects.equals(clientId, that.clientId) && Objects.equals(secret, that.secret);
    }

    @Override
    public int hashCode() {
      int result = Objects.hashCode(clientId);
      result = 31 * result + Objects.hashCode(secret);
      return result;
    }
  }

  public static NessieClientConfigSource emptyConfigSource() {
    return EMPTY_CONFIG_SOURCE;
  }

  static Path dotEnvFile() {
    return Paths.get(System.getProperty("user.dir"), ".env");
  }

  static Path nessieClientConfigFile() {
    return Paths.get(
        System.getProperty("user.dir"), ".config", "nessie", "nessie-client.properties");
  }

  private static Properties loadProperties(Path propertiesFile) {
    Properties props = new Properties();
    try (BufferedReader input = Files.newBufferedReader(propertiesFile)) {
      props.load(input);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return props;
  }
}
