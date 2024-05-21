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

  private static VersionStoreType versionStoreType(ConfigSourceInterceptorContext context) {
    ConfigValue versionStoreType = context.proceed("nessie.version.store.type");
    if (versionStoreType == null || versionStoreType.getValue() == null) {
      return VersionStoreType.IN_MEMORY;
    }
    return VersionStoreType.valueOf(versionStoreType.getValue());
  }

  private static String dataSourceName(String name) {
    if (name.equals("quarkus.datasource.active")) {
      return "default";
    }
    return name.substring("quarkus.datasource.".length(), name.length() - ".active".length());
  }

  private static String activeDataSourceName(ConfigSourceInterceptorContext context) {
    ConfigValue nessieDataSource = context.proceed("nessie.version.store.persist.jdbc.datasource");
    if (nessieDataSource == null || nessieDataSource.getValue() == null) {
      return "default";
    }
    return nessieDataSource.getValue();
  }

  private static ConfigValue newConfigValue(boolean value) {
    return ConfigValue.builder()
        .withValue(value ? "true" : "false")
        .withConfigSourceOrdinal(APPLICATION_PROPERTIES_CLASSPATH_ORDINAL + 1)
        .build();
  }
}
