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

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Fields or method parameters annotated with {@link NessieAPI} receive the API instance using the
 * "expected" (or "currently tested") Nessie client version.
 *
 * <p>Custom configurations for instances of {@link org.projectnessie.client.api.NessieApi} can be
 * provided using the repeated annotation {@link NessieApiBuilderProperty}.
 *
 * <p>
 */
@Target({ElementType.FIELD, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Inherited
public @interface NessieAPI {
  String DEFAULT_BUILDER_CLASS_NAME = "_DEFAULT_BUILDER_CLASS_NAME_";

  String builderClassName() default DEFAULT_BUILDER_CLASS_NAME;

  /**
   * Defines the target Nessie version instance in case multiple Nessie versions are running, for
   * rolling-upgrade tests.
   *
   * <ul>
   *   <li>{@link TargetVersion#TESTED} defines that the annotated element shall be provided for the
   *       currently tested and released Nessie version instance - aka rolling-upgrade-from version.
   *   <li>{@link TargetVersion#CURRENT} defines that the annotated element shall be provided for
   *       the in-tree Nessie version instance - aka rolling-upgrade-to version.
   * </ul>
   */
  TargetVersion targetVersion() default TargetVersion.TESTED;
}
