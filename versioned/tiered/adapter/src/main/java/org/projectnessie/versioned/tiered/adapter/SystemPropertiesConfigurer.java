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
package org.projectnessie.versioned.tiered.adapter;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Helper class to configure instances of {@link DatabaseAdapterConfig} via system properties for
 * microbenchmarks and other rather one-off things. Do <em>not</em> use this class in production
 * code.
 *
 * <p>System properties must start with {@code nessie.store.} followed by the name of the
 * "with-function" that takes a {@code String} or {@code int}, with "camel-case-breaks" replaced
 * with dots.
 */
public class SystemPropertiesConfigurer {
  public static <T extends DatabaseAdapterConfig>
      DatabaseAdapterConfig configureFromSystemProperties(T config) {
    return configureFromProperties(config, System::getProperty);
  }

  @SuppressWarnings("unchecked")
  public static <T extends DatabaseAdapterConfig> DatabaseAdapterConfig configureFromProperties(
      T config, Function<String, String> property) {
    List<Method> l =
        Arrays.stream(config.getClass().getMethods())
            .filter(m -> m.getName().startsWith("with"))
            .filter(m -> m.getName().length() >= 5)
            .filter(m -> Modifier.isPublic(m.getModifiers()))
            .filter(m -> DatabaseAdapterConfig.class.isAssignableFrom(m.getReturnType()))
            .filter(m -> m.getParameterTypes().length == 1)
            .filter(m -> property.apply(toPropertyName(m)) != null)
            .collect(Collectors.toList());
    try {
      for (Method m : l) {
        Class<?> type = m.getParameterTypes()[0];
        String propertyName = toPropertyName(m);
        String value = property.apply(propertyName);
        if (type == String.class) {
          config = (T) m.invoke(config, value);
        } else if (type == Integer.class || type == int.class) {
          config = (T) m.invoke(config, Integer.parseInt(value));
        }
      }
      return config;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static String toPropertyName(Method m) {
    // strip leading "with"
    return toPropertyName(m.getName().substring(4));
  }

  /** Converts from camel-case to dotted-name. */
  private static String toPropertyName(String name) {
    return "nessie.store." + name.replaceAll("([a-z])([A-Z]+)", "$1.$2").toLowerCase();
  }
}
