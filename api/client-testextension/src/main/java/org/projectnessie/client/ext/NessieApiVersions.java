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
package org.projectnessie.client.ext;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Annotation for JUnit5 test classes that need to run with a specific set of Nessie API versions.
 *
 * <p>By default, if this annotation is present all Nessie API versions will be enabled in tests,
 * but if this annotation is absent only the latest Nessie API version will be enabled.
 *
 * <p>This annotation activates {@link MultiVersionApiTest}. Actual API-specific parameter injection
 * is handled by related JUnit5 extensions, such as {@link NessieClientResolver} sub-classes.
 */
@Target({ElementType.TYPE, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@ExtendWith(MultiVersionApiTest.class)
@Inherited
public @interface NessieApiVersions {
  NessieApiVersion[] versions() default {NessieApiVersion.V1, NessieApiVersion.V2};
}
