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
package org.projectnessie.junit.engine;

import static org.projectnessie.junit.engine.MultiEnvAnnotationUtils.findMultiEnvTestExtensionsOn;

import java.util.Arrays;
import java.util.stream.Stream;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.engine.config.DefaultJupiterConfiguration;
import org.junit.jupiter.engine.extension.MutableExtensionRegistry;
import org.junit.platform.commons.util.AnnotationUtils;

/**
 * A helper class for collecting instances of {@link MultiEnvTestExtension}.
 *
 * <p>Note: those extensions may be re-instantiated by the JUnit Framework during actual test
 * execution.
 */
public class MultiEnvExtensionRegistry {
  private final MutableExtensionRegistry registry;

  public MultiEnvExtensionRegistry() {
    registry =
        MutableExtensionRegistry.createRegistryWithDefaultExtensions(
            new DefaultJupiterConfiguration(new EmptyConfigurationParameters()));
  }

  public void registerExtensions(Class<?> testClass) {
    AnnotationUtils.findRepeatableAnnotations(testClass, ExtendWith.class).stream()
        .flatMap(e -> Arrays.stream(e.value()))
        .filter(MultiEnvTestExtension.class::isAssignableFrom)
        .forEach(registry::registerExtension);
  }

  public Stream<? extends MultiEnvTestExtension> stream(Class<?> testClass) {
    return findMultiEnvTestExtensionsOn(testClass).flatMap(registry::stream);
  }
}
