
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

import com.dremio.nessie.server.ServerConfiguration;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.glassfish.hk2.api.Factory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * server configuration based on yaml/json input.
 */
public class ServerConfigurationImpl implements ServerConfiguration {

  private static final Logger logger = LoggerFactory.getLogger(ServerConfigurationImpl.class);
  private final ServerDatabaseConfigurationImpl databaseConfiguration;
  private final ServerAuthConfigurationImpl authenticationConfiguration;
  private final ServiceConfigurationImpl serviceConfiguration;
  private final String defaultTag;

  @JsonCreator
  public ServerConfigurationImpl(@JsonProperty("databaseConfiguration")
                                   ServerDatabaseConfigurationImpl databaseConfiguration,
                                 @JsonProperty("authenticationConfiguration")
                                   ServerAuthConfigurationImpl authenticationConfiguration,
                                 @JsonProperty("serviceConfiguration")
                                   ServiceConfigurationImpl serviceConfiguration,
                                 @JsonProperty("defaultTag") String defaultTag) {
    this.databaseConfiguration = databaseConfiguration;
    this.authenticationConfiguration = authenticationConfiguration;
    this.serviceConfiguration = serviceConfiguration;
    this.defaultTag = defaultTag;
  }

  public ServerConfigurationImpl() {
    databaseConfiguration = new ServerDatabaseConfigurationImpl();
    authenticationConfiguration = new ServerAuthConfigurationImpl();
    serviceConfiguration = new ServiceConfigurationImpl(19120, true, true, true);
    defaultTag = null;
  }

  @Override
  public ServerDatabaseConfiguration getDatabaseConfiguration() {
    return databaseConfiguration;
  }

  @Override
  public ServerAuthenticationConfiguration getAuthenticationConfiguration() {
    return authenticationConfiguration;
  }

  @Override
  public ServiceConfiguration getServiceConfiguration() {
    return serviceConfiguration;
  }

  @Override
  public String getDefaultTag() {
    return defaultTag;
  }

  public static class ConfigurationFactory implements Factory<ServerConfiguration> {

    private final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

    @Override
    public ServerConfiguration provide() {
      final URL config = getClass().getClassLoader().getResource("config.yaml");
      if (config == null) {
        logger.info("No config found, continuing with defaults");
        return new ServerConfigurationImpl();
      }
      try {
        return mapper.readValue(config, ServerConfigurationImpl.class);
      } catch (IOException e) {
        logger.error("Unable to read config, continuing with defaults", e);
        return new ServerConfigurationImpl();
      }
    }

    @Override
    public void dispose(ServerConfiguration configuration) {

    }
  }

  public static class ServerDatabaseConfigurationImpl implements ServerDatabaseConfiguration {

    private final String dbClassName;
    private final Map<String, String> dbProps;

    @JsonCreator
    public ServerDatabaseConfigurationImpl(@JsonProperty("dbClassName") String dbClassName,
                                           @JsonProperty("dbProps") Map<String, String> dbProps) {
      this(dbClassName, dbProps, false);
    }

    public ServerDatabaseConfigurationImpl() {
      this("com.dremio.iceberg.backend.simple.InMemory", new HashMap<>());
    }

    public ServerDatabaseConfigurationImpl(String dbClassName,
                                           Map<String, String> dbProps,
                                           boolean skip) {
      if (skip) {
        this.dbClassName = dbClassName;
        this.dbProps = dbProps;
      } else {
        ServerDatabaseConfigurationImpl x = jvmArgParse(dbClassName, dbProps);
        this.dbProps = x.dbProps;
        this.dbClassName = x.dbClassName;
      }

    }

    private static ServerDatabaseConfigurationImpl jvmArgParse(String dbClassName,
                                                               Map<String, String> dbProps) {
      Map<String, String> envProps = System.getenv().entrySet()
                                           .stream()
                                           .filter(x -> x.getKey().contains("NESSIE_DB_PROPS"))
                                           .collect(Collectors.toMap(Map.Entry::getKey,
                                                                     Map.Entry::getValue));
      Map<String, String> jvmProps = new HashMap<>();
      for (Entry<Object, Object> x : System.getProperties().entrySet()) {
        if (((String) x.getKey()).contains("NESSIE_DB_PROPS")) {
          jvmProps.put((String) x.getKey(), (String) x.getValue());
        }
      }
      dbProps.putAll(envProps);
      dbProps.putAll(jvmProps);
      return new ServerDatabaseConfigurationImpl(
        getEnv("nessie.db.impl", "NESSIE_DB_IMPL", dbClassName),
        dbProps
      );
    }

    @Override
    public String getDbClassName() {
      return dbClassName;
    }

    @Override
    public Map<String, String> getDbProps() {
      return dbProps;
    }
  }

  public static class ServerAuthConfigurationImpl implements
                                                  ServerAuthenticationConfiguration {

    private final String userServiceClassName;
    private final boolean enableLoginEndpoint;
    private final boolean enableUsersEndpoint;

    @JsonCreator
    public ServerAuthConfigurationImpl(@JsonProperty("userServiceClassName")
                                         String userServiceClassName,
                                       @JsonProperty("userServiceClassName")
                                         boolean enableLoginEndpoint,
                                       @JsonProperty("userServiceClassName")
                                         boolean enableUsersEndpoint) {
      this.userServiceClassName = userServiceClassName;
      this.enableLoginEndpoint = enableLoginEndpoint;
      this.enableUsersEndpoint = enableUsersEndpoint;
    }

    public ServerAuthConfigurationImpl() {
      this("com.dremio.iceberg.server.auth.BasicUserService", true, true);
    }

    @Override
    public String getUserServiceClassName() {
      return userServiceClassName;
    }

    @Override
    public boolean getEnableLoginEndpoint() {
      return enableLoginEndpoint;
    }

    @Override
    public boolean getEnableUsersEndpoint() {
      return enableUsersEndpoint;
    }
  }

  public static class ServiceConfigurationImpl implements ServiceConfiguration {

    private final int port;
    private final boolean ui;
    private final boolean swagger;
    private final boolean metrics;

    public ServiceConfigurationImpl() {
      this(19120, true, true, true);
    }

    @JsonCreator
    public ServiceConfigurationImpl(@JsonProperty("port") int port,
                                    @JsonProperty("ui") boolean ui,
                                    @JsonProperty("swagger") boolean swagger,
                                    @JsonProperty("metrics") boolean metrics) {
      this.port = port;
      this.ui = ui;
      this.swagger = swagger;
      this.metrics = metrics;
    }

    @Override
    public int getPort() {
      return port;
    }

    @Override
    public boolean getUi() {
      return ui;
    }

    @Override
    public boolean getSwagger() {
      return swagger;
    }

    @Override
    public boolean getMetrics() {
      return metrics;
    }
  }

  private static String getEnv(String jvmName, String envName, String def) {
    String val = System.getenv(envName);
    return System.getProperty(jvmName, val == null ? def : val);
  }
}
