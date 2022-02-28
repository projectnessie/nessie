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
package org.projectnessie.tools.compatibility.api;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/** Restrict execution of a test class or method to a range of versions. */
@Target({ElementType.TYPE, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Inherited
public @interface VersionCondition {

  /**
   * The minimum Nessie version, inclusive, that the annotated element (test class or method) shall
   * run against. To include the current in-tree Nessie version, use {@value
   * Version#CURRENT_STRING}.
   */
  String minVersion() default "";

  /**
   * The maximum Nessie version, inclusive, that the annotated element (test class or method) shall
   * run against. To exclude the current in-tree Nessie version, use {@value
   * Version#NOT_CURRENT_STRING}.
   */
  String maxVersion() default "";
}
