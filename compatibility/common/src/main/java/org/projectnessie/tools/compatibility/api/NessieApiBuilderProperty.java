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
import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.function.Function;

/**
 * Configuration properties passed to {@link org.projectnessie.client.NessieClientBuilder} via
 * {@link org.projectnessie.client.NessieClientBuilder#fromConfig(Function)}.
 *
 * <p>The implementation takes care of setting the right value for {@value
 * org.projectnessie.client.NessieConfigConstants#CONF_NESSIE_URI}.
 *
 * <p>See {@link NessieAPI}.
 */
@Target({ElementType.TYPE, ElementType.FIELD, ElementType.PARAMETER})
@Retention(RetentionPolicy.RUNTIME)
@Repeatable(NessieApiBuilderProperties.class)
@Inherited
public @interface NessieApiBuilderProperty {
  String name();

  String value();
}
