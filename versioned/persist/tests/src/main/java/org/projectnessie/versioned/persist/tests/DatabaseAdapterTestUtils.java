/*
 * Copyright (C) 2022 Dremio
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
package org.projectnessie.versioned.persist.tests;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Assertions;
import org.projectnessie.versioned.persist.adapter.DatabaseAdapterConfig;

public class DatabaseAdapterTestUtils {

  public static <T extends DatabaseAdapterConfig> void assertAdjustableConfigConvention(
      Class<T> configClass, Class<? extends T> adjustableConfigClass) {
    assertThat(adjustableConfigClass).isInterface();
    assertThat(configClass).isInterface().isAssignableFrom(adjustableConfigClass);

    List<String> missingMethods =
        Stream.of(configClass.getDeclaredMethods())
            .filter(method -> method.getName().startsWith("get"))
            .map(
                method -> {
                  String expectedSetterName = method.getName().replaceFirst("get", "with");
                  String expectedParamName = expectedSetterName.replaceFirst("with", "");
                  expectedParamName =
                      expectedParamName.substring(0, 1).toLowerCase(Locale.ROOT)
                          + expectedParamName.substring(1);
                  String expectedMethodCode =
                      adjustableConfigClass.getSimpleName()
                          + " "
                          + expectedSetterName
                          + "("
                          + method.getReturnType().getSimpleName()
                          + " "
                          + expectedParamName
                          + ");";
                  try {
                    Method setter =
                        adjustableConfigClass.getDeclaredMethod(
                            expectedSetterName, method.getReturnType());
                    if (setter.getParameters().length == 1
                        && setter.getParameters()[0].getName().equals(expectedParamName)) {
                      return Optional.<String>empty();
                    }
                  } catch (NoSuchMethodException e) {
                    // ignore
                  }
                  return Optional.of(expectedMethodCode);
                })
            .filter(Optional::isPresent)
            .map(Optional::get)
            .collect(Collectors.toList());
    if (!missingMethods.isEmpty()) {
      Assertions.fail(
          "By convention "
              + adjustableConfigClass.getSimpleName()
              + " is missing the following methods:\n"
              + String.join("\n", missingMethods));
    }
  }
}
