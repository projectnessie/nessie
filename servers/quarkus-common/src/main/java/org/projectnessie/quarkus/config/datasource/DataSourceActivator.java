/*
 * Copyright (C) 2024 Dremio
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
package org.projectnessie.quarkus.config.datasource;

import io.smallrye.config.ConfigSourceInterceptor;
import io.smallrye.config.ConfigSourceInterceptorContext;
import io.smallrye.config.ConfigValue;
import io.smallrye.config.ConfigValue.ConfigValueBuilder;
import io.smallrye.config.Priorities;
import jakarta.annotation.Priority;
import org.projectnessie.quarkus.config.VersionStoreConfig.VersionStoreType;
import org.projectnessie.quarkus.providers.storage.JdbcBackendBuilder;

/**
 * Activates a data source based on the current Nessie configuration under {@code
 * nessie.version.store.persist.jdbc.datasource}, and deactivates all other data sources.
 *
 * <p>If the version store type is not JDBC, all data sources are deactivated.
 */
@Priority(Priorities.LIBRARY + 400)
public class DataSourceActivator implements ConfigSourceInterceptor {

  /**
   * The default ordinal to use for modified {@code quarkus.datasource.*.active} properties. It must
   * be higher than the ordinal of the application.properties classpath config source (250) but
   * lower than the ordinal of the user-supplied application.properties (260).
   */
  static final int DEFAULT_ORDINAL = 251;

  private static String activeDataSourceName;
  private static VersionStoreType versionStoreType;

  @Override
  public ConfigValue getValue(ConfigSourceInterceptorContext context, String name) {
    ConfigValue value = context.proceed(name);
    if (name.startsWith("quarkus.datasource.") && name.endsWith(".active")) {
      boolean active = isDataSourceActive(context, name);
      if (value == null
          || value.getValue() == null
          || active != Boolean.parseBoolean(value.getValue())) {
        value = newConfigValue(value, active ? "true" : "false");
      }
    }
    return value;
  }

  @SuppressWarnings("deprecation")
  static boolean isDataSourceActive(ConfigSourceInterceptorContext context, String name) {
    VersionStoreType type = versionStoreType(context);
    return (type == VersionStoreType.JDBC || type == VersionStoreType.JDBC2)
        && dataSourceName(name).equals(activeDataSourceName(context));
  }

  static ConfigValue newConfigValue(ConfigValue current, String newValue) {
    ConfigValueBuilder builder = current == null ? ConfigValue.builder() : current.from();
    int ordinal = current == null ? DEFAULT_ORDINAL : current.getConfigSourceOrdinal() + 1;
    return builder.withConfigSourceOrdinal(ordinal).withValue(newValue).build();
  }

  private static synchronized VersionStoreType versionStoreType(
      ConfigSourceInterceptorContext context) {
    if (versionStoreType == null) {
      ConfigValue value = context.proceed("nessie.version.store.type");
      versionStoreType =
          value == null || value.getValue() == null
              ? VersionStoreType.IN_MEMORY
              : VersionStoreType.valueOf(value.getValue());
    }
    return versionStoreType;
  }

  private static String dataSourceName(String property) {
    if (property.equals("quarkus.datasource.active")) {
      return JdbcBackendBuilder.DEFAULT_DATA_SOURCE_NAME;
    }
    String dataSourceName = property.substring("quarkus.datasource.".length());
    dataSourceName = dataSourceName.substring(0, dataSourceName.indexOf('.'));
    return JdbcBackendBuilder.unquoteDataSourceName(dataSourceName);
  }

  private static synchronized String activeDataSourceName(ConfigSourceInterceptorContext context) {
    if (activeDataSourceName == null) {
      ConfigValue value = context.proceed("nessie.version.store.persist.jdbc.datasource");
      activeDataSourceName =
          value == null || value.getValue() == null
              ? JdbcBackendBuilder.DEFAULT_DATA_SOURCE_NAME
              : JdbcBackendBuilder.unquoteDataSourceName(value.getValue());
    }
    return activeDataSourceName;
  }
}
