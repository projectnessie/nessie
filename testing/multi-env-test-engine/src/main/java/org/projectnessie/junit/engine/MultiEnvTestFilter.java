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

import java.util.Optional;
import org.junit.jupiter.engine.descriptor.ClassBasedTestDescriptor;
import org.junit.platform.engine.FilterResult;
import org.junit.platform.engine.TestDescriptor;
import org.junit.platform.engine.UniqueId;
import org.junit.platform.launcher.PostDiscoveryFilter;

/**
 * This filter excludes multi-environment tests engines from the JUnit Jupiter Test Engine.
 *
 * <p>Background: {@link MultiEnvTestEngine} delegates test discovery to JUnit Jupiter to run each
 * test class against a certain Nessie environment. However, JUnit Jupiter itself also discovers the
 * same tests, yet those test instances would not have the expected Nessie services running during
 * execution. Those tests can be skipped, but with this filter it's nicer, because JUnit Jupiter
 * does not even attempt to run them. Therefore, the duplicate discoveries will not appear in IDEs
 * and build output - less user confusion.
 */
public class MultiEnvTestFilter implements PostDiscoveryFilter {

  static Optional<Class<?>> classFor(TestDescriptor object) {
    for (TestDescriptor d = object; d != null; d = d.getParent().orElse(null)) {
      if (d instanceof ClassBasedTestDescriptor) {
        return Optional.of(((ClassBasedTestDescriptor) d).getTestClass());
      }
    }

    return Optional.empty();
  }

  /**
   * This filter effectively routes all tests via the {@link MultiEnvTestEngine}, both actual
   * multi-env tests but also non-multi-env tests to achieve the needed thread-per-test-class
   * behavior.
   *
   * <p>"Thread-per-test-class behavior" is needed to prevent the class/class-loader leak via {@link
   * ThreadLocal}s as described in <a
   * href="https://github.com/projectnessie/nessie/issues/9441">#9441</a>.
   */
  private FilterResult filter(UniqueId id) {
    if (id.getEngineId().map("junit-jupiter"::equals).orElse(false)) {
      return FilterResult.excluded("Excluding multi-env test from Jupiter Engine: " + id);
    } else {
      return FilterResult.included(null);
    }
  }

  @Override
  public FilterResult apply(TestDescriptor test) {
    return classFor(test)
        .map(testClass -> filter(test.getUniqueId()))
        .orElseGet(() -> FilterResult.included(null)); // fallback for non-class descriptors
  }
}
