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
import org.projectnessie.quarkus.config.VersionStoreConfig.VersionStoreType;

/**
 * Activates a data source based on the current Nessie configuration under {@code
 * nessie.version.store.persist.jdbc.datasource}, and deactivates all other data sources.
 *
 * <p>If the version store type is not JDBC, all data sources are deactivated.
 */
public class DataSourceActivator implements ConfigSourceInterceptor {

  private static final int APPLICATION_PROPERTIES_CLASSPATH_ORDINAL = 250;

  private static String activeDataSourceName;
  private static VersionStoreType versionStoreType;

  @Override
  public ConfigValue getValue(ConfigSourceInterceptorContext context, String name) {
    if (name.startsWith("quarkus.datasource.") && name.endsWith(".active")) {
      boolean active = false;
      if (versionStoreType(context) == VersionStoreType.JDBC) {
        active = dataSourceName(name).equals(activeDataSourceName(context));
      }
      return newConfigValue(active);
    }
    return context.proceed(name);
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
      return "default";
    }
    String dataSourceName =
        property.substring("quarkus.datasource.".length(), property.length() - ".active".length());
    return unquote(dataSourceName);
  }

  private static synchronized String activeDataSourceName(ConfigSourceInterceptorContext context) {
    if (activeDataSourceName == null) {
      ConfigValue value = context.proceed("nessie.version.store.persist.jdbc.datasource");
      activeDataSourceName =
          value == null || value.getValue() == null ? "default" : unquote(value.getValue());
    }
    return activeDataSourceName;
  }

  private static ConfigValue newConfigValue(boolean value) {
    return ConfigValue.builder()
        .withValue(value ? "true" : "false")
        .withConfigSourceOrdinal(APPLICATION_PROPERTIES_CLASSPATH_ORDINAL + 1)
        .build();
  }

  public static String unquote(String dataSourceName) {
    if (dataSourceName.startsWith("\"") && dataSourceName.endsWith("\"")) {
      dataSourceName = dataSourceName.substring(1, dataSourceName.length() - 1);
    }
    return dataSourceName;
  }
}
