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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import org.glassfish.hk2.api.Factory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * server configuration based on yaml/json input.
 */
public class ServerConfigurationImpl implements ServerConfiguration {

  private static final Logger logger = LoggerFactory.getLogger(ServerConfigurationImpl.class);
  private final ServerDatabaseConfigurationImpl databaseConfiguration;
  private final ServerAuthenticationConfigurationImpl authenticationConfiguration;
  private final ServiceConfigurationImpl serviceConfiguration;
  private final String defaultTag;

  public ServerConfigurationImpl(ServerDatabaseConfigurationImpl databaseConfiguration,
                                 ServerAuthenticationConfigurationImpl authenticationConfiguration,
                                 ServiceConfigurationImpl serviceConfiguration,
                                 String defaultTag) {
    this.databaseConfiguration = databaseConfiguration;
    this.authenticationConfiguration = authenticationConfiguration;
    this.serviceConfiguration = serviceConfiguration;
    this.defaultTag = defaultTag;
  }

  public ServerConfigurationImpl() {
    databaseConfiguration = new ServerDatabaseConfigurationImpl();
    authenticationConfiguration = new ServerAuthenticationConfigurationImpl();
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
      dbProps = new HashMap<>();
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

  public static class ServerAuthenticationConfigurationImpl implements
                                                            ServerAuthenticationConfiguration {

    private final String userServiceClassName;
    private final boolean enableLoginEndpoint;
    private final boolean enableUsersEndpoint;

    public ServerAuthenticationConfigurationImpl(String userServiceClassName,
                                                 boolean enableLoginEndpoint,
                                                 boolean enableUsersEndpoint) {
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

    public ServiceConfigurationImpl(int port,
                                    boolean ui,
                                    boolean swagger,
                                    boolean metrics) {
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
}
