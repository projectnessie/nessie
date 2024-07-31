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

import static org.projectnessie.quarkus.config.datasource.DataSourceActivator.isDataSourceActive;
import static org.projectnessie.quarkus.config.datasource.DataSourceActivator.newConfigValue;

import io.smallrye.config.ConfigSourceInterceptor;
import io.smallrye.config.ConfigSourceInterceptorContext;
import io.smallrye.config.ConfigValue;
import io.smallrye.config.Priorities;
import jakarta.annotation.Priority;

/**
 * Interceptor to change the JDBC URL from {@code jdbc:mysql:} to {@code jdbc:mariadb:} if the
 * "mysql" datasource is active.
 */
@Priority(Priorities.LIBRARY + 300)
public class JdbcUrlInterceptor implements ConfigSourceInterceptor {

  @Override
  public ConfigValue getValue(ConfigSourceInterceptorContext context, String name) {
    ConfigValue value = context.proceed(name);
    if (name.equals("quarkus.datasource.mysql.jdbc.url")) {
      if (isDataSourceActive(context, name)) {
        if (value != null) {
          String url = value.getValue();
          if (url.startsWith("jdbc:mysql:")) {
            url = url.replace("jdbc:mysql:", "jdbc:mariadb:");
            value = newConfigValue(value, url);
          }
        }
      }
    }
    return value;
  }
}
