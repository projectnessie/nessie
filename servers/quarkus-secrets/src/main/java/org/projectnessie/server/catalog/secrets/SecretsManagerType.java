/*
 * Copyright (C) 2024 Dremio
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
package org.projectnessie.server.catalog.secrets;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static java.util.Objects.requireNonNull;

import jakarta.enterprise.util.AnnotationLiteral;
import jakarta.inject.Qualifier;
import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import org.projectnessie.quarkus.config.QuarkusSecretsConfig.ExternalSecretsManagerType;

/** Secrets manager type qualifier. */
@Target({TYPE, METHOD, PARAMETER, FIELD})
@Retention(RUNTIME)
@Documented
@Qualifier
public @interface SecretsManagerType {
  /** Gets the secrets supplier type. */
  String value();

  /** Supports inline instantiation of the {@link SecretsManagerType} qualifier. */
  @SuppressWarnings("ClassExplicitlyAnnotation")
  final class Literal extends AnnotationLiteral<SecretsManagerType> implements SecretsManagerType {

    private final ExternalSecretsManagerType value;

    public Literal(String value) {
      this(ExternalSecretsManagerType.valueOf(value));
    }

    public Literal(ExternalSecretsManagerType value) {
      this.value = requireNonNull(value);
    }

    @Override
    public String value() {
      return value.name();
    }
  }
}
