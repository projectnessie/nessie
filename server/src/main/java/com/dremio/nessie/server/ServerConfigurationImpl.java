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

package com.dremio.nessie.server;

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

  /**
   * Constructor for Jackson.
   */
  @JsonCreator
  public ServerConfigurationImpl(@JsonProperty("databaseConfiguration")
                                   ServerDatabaseConfigurationImpl databaseConfiguration,
                                 @JsonProperty("authenticationConfiguration")
                                   ServerAuthConfigurationImpl authenticationConfiguration,
                                 @JsonProperty("serviceConfiguration")
                                   ServiceConfigurationImpl serviceConfiguration,
                                 @JsonProperty("defaultTag")
                                   String defaultTag) {
    this.databaseConfiguration = databaseConfiguration;
    this.authenticationConfiguration = authenticationConfiguration;
    this.serviceConfiguration = serviceConfiguration;
    this.defaultTag = defaultTag;
  }

  /**
   * no-args constructor.
   */
  public ServerConfigurationImpl() {
    databaseConfiguration = new ServerDatabaseConfigurationImpl();
    authenticationConfiguration = new ServerAuthConfigurationImpl();
    serviceConfiguration = new ServiceConfigurationImpl();
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
      } catch (IOException | NullPointerException | IllegalArgumentException e) {
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

    /**
     * Constructor for Jackson.
     */
    @JsonCreator
    public ServerDatabaseConfigurationImpl(@JsonProperty("dbClassName") String dbClassName,
                                           @JsonProperty("dbProps") Map<String, String> dbProps) {
      this(dbClassName, dbProps, false);
    }

    /**
     * construct database config. Including any jvm or env args.
     */
    public ServerDatabaseConfigurationImpl(String dbClassName,
                                           Map<String, String> dbProps,
                                           boolean skip) {
      if (skip) {
        this.dbClassName = dbClassName;
        this.dbProps = dbProps;
      } else {
        ServerDatabaseConfigurationImpl pair = jvmArgsOverride(dbClassName, dbProps);
        this.dbClassName = pair.dbClassName;
        this.dbProps = pair.dbProps;
      }
    }

    public ServerDatabaseConfigurationImpl() {
      this("com.dremio.nessie.backend.simple.InMemory", new HashMap<>());
    }

    @Override
    public String getDbClassName() {
      return dbClassName;
    }

    @Override
    public Map<String, String> getDbProps() {
      return dbProps;
    }

    private static ServerDatabaseConfigurationImpl jvmArgsOverride(String dbClassName,
                                                                   Map<String, String> dbProps) {
      String db = getProperty("nessie.db.className", "NESSIE_DB_CLASSNAME", dbClassName);
      Map<String, String> props = System.getProperties()
                                        .entrySet()
                                        .stream()
                                        .filter(x -> ((String) x.getKey()).contains(
                                          "nessie.db.props"))
                                        .collect(Collectors.toMap(x -> (String) x.getKey(),
                                            x -> (String) x.getValue()));
      Map<String, String> envs = System.getenv()
                                       .entrySet()
                                       .stream()
                                       .filter(x -> x.getKey().contains("NESSIE_DB_PROPS"))
                                       .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
      for (String prop : envs.keySet()) {
        dbProps.put(prop.replace("NESSIE_DB_PROPS_", "").toLowerCase(), envs.get(prop));
      }
      for (String prop : props.keySet()) {
        dbProps.put(prop.replace("nessie.db.props.", ""), props.get(prop));
      }
      return new ServerDatabaseConfigurationImpl(db, dbProps, true);
    }
  }

  public static class ServerAuthConfigurationImpl implements
                                                  ServerAuthenticationConfiguration {

    private final String userServiceClassName;
    private final boolean enableLoginEndpoint;
    private final boolean enableUsersEndpoint;

    /**
     * Constructor for Jackson.
     */
    @JsonCreator
    public ServerAuthConfigurationImpl(@JsonProperty("userServiceClassName")
                                         String userServiceClassName,
                                       @JsonProperty("enableLoginEndpoint")
                                         boolean enableLoginEndpoint,
                                       @JsonProperty("enableUsersEndpoint")
                                         boolean enableUsersEndpoint) {
      this.userServiceClassName = userServiceClassName;
      this.enableLoginEndpoint = enableLoginEndpoint;
      this.enableUsersEndpoint = enableUsersEndpoint;
    }

    public ServerAuthConfigurationImpl() {
      this("com.dremio.nessie.server.auth.BasicUserService", true, true);
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

    /**
     * Constructor for Jackson.
     */
    @JsonCreator
    public ServiceConfigurationImpl(@JsonProperty("port") int port,
                                    @JsonProperty("ui") boolean ui,
                                    @JsonProperty("swagger") boolean swagger,
                                    @JsonProperty("metrics") boolean metrics) {
      this(port, ui, swagger, metrics, false);
    }

    /**
     * construct service config. Including any jvm or env args.
     */
    public ServiceConfigurationImpl(int port,
                                    boolean ui,
                                    boolean swagger,
                                    boolean metrics,
                                    boolean skip) {
      if (skip) {
        this.port = port;
        this.ui = ui;
        this.swagger = swagger;
        this.metrics = metrics;
      } else {
        ServiceConfigurationImpl service = jvmArgsOverride(port, ui, swagger, metrics);
        this.port = service.port;
        this.ui = service.ui;
        this.swagger = service.swagger;
        this.metrics = service.metrics;
      }
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

    private static ServiceConfigurationImpl jvmArgsOverride(int port,
                                                            boolean ui,
                                                            boolean swagger,
                                                            boolean metrics) {
      String portVal = getProperty("nessie.service.port",
                                   "NESSIE_SERVICE_PORT",
                                   String.valueOf(port));
      String uiVal = getProperty("nessie.service.ui", "NESSIE_SERVICE_UI", String.valueOf(ui));
      String swaggerVal = getProperty("nessie.service.swagger",
                                      "NESSIE_SERVICE_SWAGGER",
                                      String.valueOf(swagger));
      String metricsVal = getProperty("nessie.service.metrics",
                                      "NESSIE_SERVICE_METRICS",
                                      String.valueOf(metrics));
      return new ServiceConfigurationImpl(Integer.parseInt(portVal),
                                          Boolean.parseBoolean(uiVal),
                                          Boolean.parseBoolean(swaggerVal),
                                          Boolean.parseBoolean(metricsVal),
                                          true);
    }
  }

  private static String getProperty(String jvm, String env, String def) {
    String envVar = System.getenv(env);
    return System.getProperty(jvm, envVar == null ? def : envVar);
  }
}
