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
package org.projectnessie.versioned.persist.tests.extension;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({ElementType.FIELD, ElementType.PARAMETER})
@Retention(RetentionPolicy.RUNTIME)
@Inherited
public @interface NessieDbAdapter {

  /**
   * Optional: name of method to update the {@link
   * org.projectnessie.versioned.persist.adapter.DatabaseAdapterConfig configuration} for the {@link
   * org.projectnessie.versioned.persist.adapter.DatabaseAdapter}.
   *
   * <p>The method must be
   *
   * <ul>
   *   <li>static
   *   <li>not private
   *   <li>have a single parameter {@code
   *       org.projectnessie.versioned.persist.adapter.AdjustableDatabaseAdapterConfig}
   *   <li>return {@code org.projectnessie.versioned.persist.adapter.DatabaseAdapterConfig}
   * </ul>
   *
   * <p>Example:
   *
   * <pre><code>
   *   &#64;NessieDbAdapter(configMethod = "applyTestClock")
   *   protected static DatabaseAdapter databaseAdapter;
   *
   *   static DatabaseAdapterConfig applyTestClock(AdjustableDatabaseAdapterConfig config) {
   *     return config.withClock(TEST_CLOCK);
   *   }
   * </code></pre>
   */
  String configMethod() default "";
}
