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
package org.projectnessie.tools.compatibility.internal;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

final class Configurations {
  private Configurations() {}

  static <T> void backendConfigBuilderApply(
      Class<?> builderType, T config, Function<String, String> property) {
    List<Method> methods =
        Arrays.stream(config.getClass().getDeclaredMethods())
            .filter(m -> Modifier.isPublic(m.getModifiers()))
            .filter(m -> builderType.isAssignableFrom(m.getReturnType()))
            .filter(m -> m.getParameterCount() == 1)
            .filter(m -> property.apply(toPropertyName(m.getName())) != null)
            .collect(Collectors.toList());
    configApply(config, m -> property.apply(toPropertyName(m.getName())), methods);
  }

  @SuppressWarnings("unchecked")
  private static <T> void configApply(
      T config, Function<Method, String> property, List<Method> methods) {
    try {
      for (Method m : methods) {
        Class<?> type = m.getParameterTypes()[0];
        String value = property.apply(m);
        if (type == String.class) {
          config = (T) m.invoke(config, value);
        } else if (type == Integer.class || type == int.class) {
          config = (T) m.invoke(config, Integer.parseInt(value));
        } else if (type == Long.class || type == long.class) {
          config = (T) m.invoke(config, Long.parseLong(value));
        } else if (type == Float.class || type == float.class) {
          config = (T) m.invoke(config, Float.parseFloat(value));
        } else if (type == Double.class || type == double.class) {
          config = (T) m.invoke(config, Double.parseDouble(value));
        } else if (type == Boolean.class || type == boolean.class) {
          config = (T) m.invoke(config, Boolean.parseBoolean(value));
        } else if (type == Path.class) {
          config = (T) m.invoke(config, Paths.get(value));
        } else {
          throw new IllegalArgumentException(
              "Cannot convert the string value for " + m.getName() + " to " + type.getName());
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /** Converts from camel-case to dotted-name. */
  private static String toPropertyName(String name) {
    return CONFIG_NAME_PREFIX
        + TO_PROPERTY_NAME_PATTERN.matcher(name).replaceAll("$1.$2").toLowerCase(Locale.ROOT);
  }

  public static final String CONFIG_NAME_PREFIX = "nessie.store.";
  private static final Pattern TO_PROPERTY_NAME_PATTERN = Pattern.compile("([a-z])([A-Z]+)");
}
