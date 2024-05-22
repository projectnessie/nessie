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

  private Optional<Class<?>> classFor(TestDescriptor object) {
    for (TestDescriptor d = object; d != null; d = d.getParent().orElse(null)) {
      if (d instanceof ClassBasedTestDescriptor) {
        return Optional.of(((ClassBasedTestDescriptor) d).getTestClass());
      }
    }

    return Optional.empty();
  }

  private FilterResult filter(Class<?> testClass, UniqueId id) {
    boolean isJunitEngine = id.getEngineId().map("junit-jupiter"::equals).orElse(false);
    boolean isMultiEnvTest =
        findMultiEnvTestExtensionsOn(testClass)
            .map(MultiEnvAnnotationUtils::segmentTypeOf)
            .findAny()
            .isPresent();

    if (isJunitEngine) {
      if (isMultiEnvTest) {
        return FilterResult.excluded("Excluding multi-env test from Jupiter Engine: " + id);
      } else {
        return FilterResult.included(null);
      }
    } else {
      if (isMultiEnvTest) {
        return FilterResult.included(null);
      } else {
        return FilterResult.excluded("Excluding unmatched multi-env test: " + id);
      }
    }
  }

  @Override
  public FilterResult apply(TestDescriptor test) {
    return classFor(test)
        .map(testClass -> filter(testClass, test.getUniqueId()))
        .orElseGet(() -> FilterResult.included(null)); // fallback for non-class descriptors
  }
}
