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

import static org.junit.platform.commons.util.AnnotationUtils.findAnnotatedFields;
import static org.junit.platform.commons.util.AnnotationUtils.findAnnotation;
import static org.junit.platform.commons.util.ReflectionUtils.isStatic;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.function.Function;
import java.util.function.Predicate;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.projectnessie.tools.compatibility.api.NessieAPI;
import org.projectnessie.tools.compatibility.api.TargetVersion;

public final class AnnotatedFields {
  private AnnotatedFields() {}

  static void populateNessieApiFields(
      ExtensionContext context,
      Object instance,
      TargetVersion targetVersion,
      Function<Field, Object> fieldValue) {
    populateAnnotatedFields(
        context, instance, NessieAPI.class, a -> a.targetVersion() == targetVersion, fieldValue);
  }

  public static <A extends Annotation> void populateAnnotatedFields(
      ExtensionContext context,
      Object instance,
      Class<A> annotationType,
      Predicate<A> annotationFilter,
      Function<Field, Object> fieldValue) {
    Class<?> clazz = context.getRequiredTestClass();
    boolean isStatic = instance == null;

    Predicate<Field> staticPredicate = field -> isStatic == isStatic(field);
    Predicate<Field> annotationPredicate =
        field -> findAnnotation(field, annotationType).map(annotationFilter::test).orElse(true);

    findAnnotatedFields(clazz, annotationType, staticPredicate.and(annotationPredicate))
        .forEach(f -> setField(f, instance, fieldValue.apply(f)));
  }

  private static void setField(Field field, Object instance, Object value) {
    field.setAccessible(true);
    try {
      if (Modifier.isStatic(field.getModifiers())) {
        if (instance != null) {
          return;
        }
      } else {
        if (instance == null) {
          return;
        }
      }
      field.set(instance, value);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }
}
