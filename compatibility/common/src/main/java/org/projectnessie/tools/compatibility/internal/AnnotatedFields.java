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
import static org.junit.platform.commons.util.ReflectionUtils.isStatic;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.function.Function;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.projectnessie.tools.compatibility.api.NessieAPI;

final class AnnotatedFields {
  private AnnotatedFields() {}

  static void populateNessieAnnotatedFields(
      ExtensionContext context, Object instance, Function<Field, Object> fieldValue) {
    populateAnnotatedFields(context, instance, NessieAPI.class, fieldValue);
  }

  static void populateAnnotatedFields(
      ExtensionContext context,
      Object instance,
      Class<? extends Annotation> annotationType,
      Function<Field, Object> fieldValue) {
    Class<?> clazz = context.getRequiredTestClass();
    boolean isStatic = instance == null;

    findAnnotatedFields(clazz, annotationType, m -> isStatic == isStatic(m))
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
