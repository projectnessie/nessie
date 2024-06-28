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
package org.projectnessie.quarkus.providers.versionstore;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import jakarta.enterprise.util.AnnotationLiteral;
import jakarta.inject.Qualifier;
import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import org.projectnessie.quarkus.config.VersionStoreConfig.VersionStoreType;

/** Store type qualifier for {@code VersionStoreFactory} classes. */
@Target({TYPE, METHOD, PARAMETER, FIELD})
@Retention(RUNTIME)
@Documented
@Qualifier
public @interface StoreType {
  /** Gets the store type. */
  VersionStoreType value();

  /** Supports inline instantiation of the {@link StoreType} qualifier. */
  final class Literal extends AnnotationLiteral<StoreType> implements StoreType {

    private final VersionStoreType value;

    public Literal(VersionStoreType value) {
      this.value = value;
    }

    @Override
    public VersionStoreType value() {
      return value;
    }
  }
}
