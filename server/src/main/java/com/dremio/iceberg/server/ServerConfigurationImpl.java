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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.net.URL;
import java.util.Map;
import org.glassfish.hk2.api.Factory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServerConfigurationImpl implements ServerConfiguration {
  private static final Logger logger = LoggerFactory.getLogger(ServerConfigurationImpl.class);
  private ServerDatabaseConfigurationImpl databaseBackend;
  private ServerAuthenticationConfigurationImpl authorizationBackend;
  private ServiceConfigurationImpl server;
  private String defaultTag;

  public ServerConfigurationImpl(
    @JsonProperty("databaseBackend") ServerDatabaseConfigurationImpl databaseBackend,
    @JsonProperty("authorizationBackend")
      ServerAuthenticationConfigurationImpl authorizationBackend,
    @JsonProperty("server") ServiceConfigurationImpl serviceConfiguration,
    @JsonProperty("defaultTag") String defaultTag) {
    this.databaseBackend = databaseBackend;
    this.authorizationBackend = authorizationBackend;
    this.server = serviceConfiguration;
    this.defaultTag = defaultTag;
  }

  public ServerConfigurationImpl() {
    databaseBackend = new ServerDatabaseConfigurationImpl();
    authorizationBackend = new ServerAuthenticationConfigurationImpl();
    server = new ServiceConfigurationImpl(19120, true, true, true);
    defaultTag = null;
  }

  @Override
  public ServerDatabaseConfiguration getDatabaseConfiguration() {
    return databaseBackend;
  }

  @Override
  public ServerAuthenticationConfiguration getAuthenticationConfiguration() {
    return authorizationBackend;
  }

  @Override
  public ServiceConfiguration getServiceConfiguration() {
    return server;
  }

  @Override
  public String getDefaultTag() {
    return defaultTag;
  }

  public static class ConfigurationFactory implements Factory<ServerConfiguration> {
    private final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

    @Override
    public ServerConfiguration provide() {
      try {
        URL config = getClass().getClassLoader().getResource("config.yaml");
        return mapper.readValue(config, ServerConfigurationImpl.class);
      } catch (IOException | NullPointerException e) {
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

    public ServerDatabaseConfigurationImpl() {
      dbClassName = "com.dremio.iceberg.backend.simple.InMemory";
      dbProps = Maps.newHashMap();
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

  public static class ServerAuthenticationConfigurationImpl
    implements ServerAuthenticationConfiguration {
    private final String userServiceClassName;
    private final boolean enableLoginEndpoint;
    private final boolean enableUsersEndpoint;

    public ServerAuthenticationConfigurationImpl(
      @JsonProperty("userServiceClassName") String userServiceClassName,
      @JsonProperty("enableLoginEndpoint") boolean enableLoginEndpoint,
      @JsonProperty("enableUsersEndpoint") boolean enableUsersEndpoint) {
      this.userServiceClassName = userServiceClassName;
      this.enableLoginEndpoint = enableLoginEndpoint;
      this.enableUsersEndpoint = enableUsersEndpoint;
    }

    public ServerAuthenticationConfigurationImpl() {
      userServiceClassName = "com.dremio.iceberg.server.auth.BasicUserService";
      enableLoginEndpoint = true;
      enableUsersEndpoint = true;
    }

    @Override
    public String getUserServiceClassName() {
      return userServiceClassName;
    }

    @Override
    public boolean loginEndpointEnabled() {
      return enableLoginEndpoint;
    }

    @Override
    public boolean userEndpointEnabled() {
      return enableUsersEndpoint;
    }
  }

  public static class ServiceConfigurationImpl implements ServiceConfiguration {
    private final int port;
    private final boolean ui;
    private final boolean swagger;
    private final boolean metrics;

    public ServiceConfigurationImpl(
      @JsonProperty("port") int port,
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
    public boolean uiEnabled() {
      return ui;
    }

    @Override
    public boolean swaggerEnabled() {
      return swagger;
    }

    @Override
    public boolean metricsEnbled() {
      return metrics;
    }
  }
}
