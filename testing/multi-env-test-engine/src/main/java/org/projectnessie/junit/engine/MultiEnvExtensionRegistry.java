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

import static org.projectnessie.junit.engine.JUnitCompat.newDefaultJupiterConfiguration;

import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Stream;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.Extension;
import org.junit.jupiter.engine.descriptor.ClassBasedTestDescriptor;
import org.junit.jupiter.engine.extension.MutableExtensionRegistry;
import org.junit.platform.commons.util.AnnotationUtils;
import org.junit.platform.engine.EngineDiscoveryRequest;
import org.junit.platform.engine.TestDescriptor;

/**
 * A helper class for collecting instances of {@link MultiEnvTestExtension}.
 *
 * <p>Note: those extensions may be re-instantiated by the JUnit Framework during actual test
 * execution.
 */
public class MultiEnvExtensionRegistry {
  private final MutableExtensionRegistry registry;

  private final Set<TestDescriptor> probablyNotMultiEnv = new LinkedHashSet<>();

  public MultiEnvExtensionRegistry(EngineDiscoveryRequest discoveryRequest) {
    this.registry =
        MutableExtensionRegistry.createRegistryWithDefaultExtensions(
            newDefaultJupiterConfiguration(new EmptyConfigurationParameters(), discoveryRequest));
  }

  public void registerExtensions(TestDescriptor descriptor) {
    AtomicBoolean multiEnv = new AtomicBoolean(false);

    findMultiEnvExtensions(descriptor)
        .peek(x -> multiEnv.set(true))
        .forEach(registry::registerExtension);

    if (!multiEnv.get()) {
      probablyNotMultiEnv.add(descriptor);
    }
  }

  public static boolean isMultiEnvClass(TestDescriptor descriptor) {
    return findMultiEnvExtensions(descriptor).findFirst().isPresent();
  }

  private static Stream<Class<? extends Extension>> findMultiEnvExtensions(
      TestDescriptor descriptor) {
    if (descriptor instanceof ClassBasedTestDescriptor) {
      var classBased = (ClassBasedTestDescriptor) descriptor;
      var testClass = classBased.getTestClass();
      return AnnotationUtils.findRepeatableAnnotations(testClass, ExtendWith.class).stream()
          .flatMap(e -> Arrays.stream(e.value()))
          .filter(MultiEnvTestExtension.class::isAssignableFrom);
    }
    return Stream.empty();
  }

  public Stream<TestDescriptor> probablyNotMultiEnv() {
    return probablyNotMultiEnv.stream();
  }

  public Stream<MultiEnvTestExtension> stream() {
    return registry.stream(MultiEnvTestExtension.class);
  }

  public Stream<? extends MultiEnvTestExtension> stream(Class<?> testClass) {
    Set<ExtendWith> annotations = new HashSet<>();
    // Find annotations following the class nesting chain
    for (Class<?> cl = testClass; cl != null; cl = cl.getDeclaringClass()) {
      annotations.addAll(AnnotationUtils.findRepeatableAnnotations(cl, ExtendWith.class));
    }

    @SuppressWarnings("unchecked")
    Stream<? extends MultiEnvTestExtension> r =
        (Stream<? extends MultiEnvTestExtension>)
            annotations.stream()
                .flatMap(e -> Arrays.stream(e.value()))
                .filter(MultiEnvTestExtension.class::isAssignableFrom)
                .flatMap(registry::stream);
    return r;
  }
}
