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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.util.function.Function;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.projectnessie.tools.compatibility.api.NessieAPI;
import org.projectnessie.tools.compatibility.api.NessieVersion;

@SuppressWarnings({"unchecked", "rawtypes", "Convert2Lambda"})
class TestAnnotatedFields {
  @BeforeEach
  void clear() {
    AnnotatedFieldsTarget.nessieApiAnnotatedStatic = null;
    AnnotatedFieldsTarget.nessieVersionAnnotatedStatic = null;
  }

  @Test
  void nessieApiInjectionStatic() throws Exception {
    ExtensionContext extensionContext = mock(ExtensionContext.class);
    when(extensionContext.getRequiredTestClass()).thenReturn((Class) AnnotatedFieldsTarget.class);

    Function<Field, Object> fieldToObject =
        new Function<Field, Object>() {
          @Override
          public Object apply(Field field) {
            return "hello";
          }
        };
    fieldToObject = spy(fieldToObject);

    AnnotatedFields.populateNessieAnnotatedFields(extensionContext, null, fieldToObject);

    assertThat(AnnotatedFieldsTarget.nessieApiAnnotatedStatic).isEqualTo("hello");
    assertThat(AnnotatedFieldsTarget.nessieVersionAnnotatedStatic).isNull();
    verify(fieldToObject)
        .apply(AnnotatedFieldsTarget.class.getDeclaredField("nessieApiAnnotatedStatic"));
  }

  @Test
  void genericInjectionStatic() throws Exception {
    ExtensionContext extensionContext = mock(ExtensionContext.class);
    when(extensionContext.getRequiredTestClass()).thenReturn((Class) AnnotatedFieldsTarget.class);

    Function<Field, Object> fieldToObject =
        new Function<Field, Object>() {
          @Override
          public Object apply(Field field) {
            return "hello";
          }
        };
    fieldToObject = spy(fieldToObject);

    AnnotatedFields.populateAnnotatedFields(
        extensionContext, null, NessieVersion.class, fieldToObject);

    assertThat(AnnotatedFieldsTarget.nessieVersionAnnotatedStatic).isEqualTo("hello");
    assertThat(AnnotatedFieldsTarget.nessieApiAnnotatedStatic).isNull();
    verify(fieldToObject)
        .apply(AnnotatedFieldsTarget.class.getDeclaredField("nessieVersionAnnotatedStatic"));
  }

  @Test
  void nessieApiInjectionInstance() throws Exception {
    ExtensionContext extensionContext = mock(ExtensionContext.class);
    when(extensionContext.getRequiredTestClass()).thenReturn((Class) AnnotatedFieldsTarget.class);

    Function<Field, Object> fieldToObject =
        new Function<Field, Object>() {
          @Override
          public Object apply(Field field) {
            return "hello";
          }
        };
    fieldToObject = spy(fieldToObject);

    AnnotatedFieldsTarget instance = new AnnotatedFieldsTarget();

    AnnotatedFields.populateNessieAnnotatedFields(extensionContext, instance, fieldToObject);

    assertThat(instance.nessieApiAnnotatedInstance).isEqualTo("hello");
    assertThat(instance.nessieVersionAnnotatedInstance).isNull();
    assertThat(AnnotatedFieldsTarget.nessieApiAnnotatedStatic).isNull();
    assertThat(AnnotatedFieldsTarget.nessieVersionAnnotatedStatic).isNull();
    verify(fieldToObject)
        .apply(AnnotatedFieldsTarget.class.getDeclaredField("nessieApiAnnotatedInstance"));
  }

  @Test
  void genericInjectionInstance() throws Exception {
    ExtensionContext extensionContext = mock(ExtensionContext.class);
    when(extensionContext.getRequiredTestClass()).thenReturn((Class) AnnotatedFieldsTarget.class);

    Function<Field, Object> fieldToObject =
        new Function<Field, Object>() {
          @Override
          public Object apply(Field field) {
            return "hello";
          }
        };
    fieldToObject = spy(fieldToObject);

    AnnotatedFieldsTarget instance = new AnnotatedFieldsTarget();

    AnnotatedFields.populateAnnotatedFields(
        extensionContext, instance, NessieVersion.class, fieldToObject);

    assertThat(instance.nessieApiAnnotatedInstance).isNull();
    assertThat(instance.nessieVersionAnnotatedInstance).isEqualTo("hello");
    assertThat(AnnotatedFieldsTarget.nessieApiAnnotatedStatic).isNull();
    assertThat(AnnotatedFieldsTarget.nessieVersionAnnotatedStatic).isNull();
    verify(fieldToObject)
        .apply(AnnotatedFieldsTarget.class.getDeclaredField("nessieVersionAnnotatedInstance"));
  }

  static class AnnotatedFieldsTarget {
    static @NessieAPI Object nessieApiAnnotatedStatic;
    @NessieAPI Object nessieApiAnnotatedInstance;
    static @NessieVersion Object nessieVersionAnnotatedStatic;
    @NessieVersion Object nessieVersionAnnotatedInstance;
  }
}
