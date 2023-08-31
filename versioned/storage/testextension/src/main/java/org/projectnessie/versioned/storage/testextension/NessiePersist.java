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
package org.projectnessie.versioned.storage.testextension;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.projectnessie.versioned.storage.common.config.StoreConfig;
import org.projectnessie.versioned.storage.common.persist.Persist;

@Target({ElementType.FIELD, ElementType.PARAMETER})
@Retention(RetentionPolicy.RUNTIME)
@Inherited
public @interface NessiePersist {

  /**
   * Optional: name of method to update the {@linkplain StoreConfig.Adjustable configuration} for
   * the {@link Persist}.
   *
   * <p>The method must be
   *
   * <ul>
   *   <li>static
   *   <li>not private
   *   <li>have a single parameter {@link StoreConfig.Adjustable}
   *   <li>return {@link StoreConfig.Adjustable}
   * </ul>
   *
   * <p>Example:
   *
   * <pre><code>
   *   &#64;NessiePersist(configMethod = "applyTestClock")
   *   protected static Persist persist;
   *
   *   static StoreConfig.Adjustable applyTestClock(StoreConfig.Adjustable config) {
   *     return ...
   *   }
   * </code></pre>
   */
  String configMethod() default "";

  /** Whether to initialize the adapter, defaults to {@code true}. */
  boolean initializeRepo() default true;
}
