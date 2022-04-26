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
package org.projectnessie.tools.compatibility.internal;

import static org.projectnessie.tools.compatibility.internal.AbstractMultiVersionExtension.multiVersionExtensionsForTestClass;

import java.util.stream.Stream;
import org.junit.jupiter.engine.descriptor.ClassBasedTestDescriptor;
import org.junit.platform.engine.FilterResult;
import org.junit.platform.engine.TestDescriptor;
import org.junit.platform.launcher.PostDiscoveryFilter;

/**
 * Filter which excludes certain engines, by default JUnit Jupiter.
 *
 * <p>Background: {@link MultiNessieVersionsTestEngine} delegates test discovery to JUnit Jupiter to
 * run each test class against a certain Nessie version. But JUnit Jupiter "itself" also discovers
 * and runs the same tests, but those test instances do not have the expected Nessie client/server
 * running. Those tests are skipped, but with this filter it's nicer, because JUnit Jupiter does not
 * even run and therefore not appear - less user confusion.
 */
public class ExcludeJunitEnginesFilter implements PostDiscoveryFilter {

  public ExcludeJunitEnginesFilter() {}

  @Override
  public FilterResult apply(TestDescriptor object) {
    if (object instanceof ClassBasedTestDescriptor) {
      ClassBasedTestDescriptor classBased = ((ClassBasedTestDescriptor) object);
      Class<?> testClass = classBased.getTestClass();
      long count = multiVersionExtensionsForTestClass(Stream.of(testClass)).count();
      if (count > 1) {
        // Sanity check, it's illegal to have multiple nessie-compatibility extensions on one test
        // class
        throw new IllegalStateException(
            String.format(
                "Test class %s contains more than one Nessie multi-version extension",
                testClass.getName()));
      }
      if (count == 1) {
        // The test class is annotated with multi-nessie-version extension, so skip the test class
        // via JUnit itself.
        return FilterResult.excluded(
            "Skipping JUnit Jupiter for test class " + testClass.getName());
      }
    }
    return FilterResult.included(null);
  }
}
