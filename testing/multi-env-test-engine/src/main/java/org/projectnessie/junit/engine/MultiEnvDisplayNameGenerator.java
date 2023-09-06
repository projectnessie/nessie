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
package org.projectnessie.junit.engine;

import java.lang.reflect.Method;
import org.junit.jupiter.api.DisplayNameGenerator;

public class MultiEnvDisplayNameGenerator implements DisplayNameGenerator {

  private final DisplayNameGenerator delegate;
  private final String environment;

  public MultiEnvDisplayNameGenerator(DisplayNameGenerator delegate, String environment) {
    this.delegate = delegate;
    this.environment = environment;
  }

  @Override
  public String generateDisplayNameForClass(Class<?> testClass) {
    return delegate.generateDisplayNameForClass(testClass) + " [" + environment + "]";
  }

  @Override
  public String generateDisplayNameForNestedClass(Class<?> nestedClass) {
    return delegate.generateDisplayNameForNestedClass(nestedClass) + " [" + environment + "]";
  }

  @Override
  public String generateDisplayNameForMethod(Class<?> testClass, Method testMethod) {
    return delegate.generateDisplayNameForMethod(testClass, testMethod) + " [" + environment + "]";
  }
}
