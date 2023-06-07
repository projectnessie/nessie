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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.util.function.Function;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.projectnessie.tools.compatibility.api.NessieAPI;
import org.projectnessie.tools.compatibility.api.NessieVersion;
import org.projectnessie.tools.compatibility.api.TargetVersion;

@SuppressWarnings({"unchecked", "rawtypes", "Convert2Lambda"})
@ExtendWith(SoftAssertionsExtension.class)
class TestAnnotatedFields {
  @InjectSoftAssertions protected SoftAssertions soft;

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
        new Function<>() {
          @Override
          public Object apply(Field field) {
            return "hello";
          }
        };
    fieldToObject = spy(fieldToObject);

    AnnotatedFields.populateNessieApiFields(
        extensionContext, null, TargetVersion.TESTED, fieldToObject);

    soft.assertThat(AnnotatedFieldsTarget.nessieApiAnnotatedStatic).isEqualTo("hello");
    soft.assertThat(AnnotatedFieldsTarget.nessieVersionAnnotatedStatic).isNull();
    verify(fieldToObject)
        .apply(AnnotatedFieldsTarget.class.getDeclaredField("nessieApiAnnotatedStatic"));
  }

  @Test
  void genericInjectionStatic() throws Exception {
    ExtensionContext extensionContext = mock(ExtensionContext.class);
    when(extensionContext.getRequiredTestClass()).thenReturn((Class) AnnotatedFieldsTarget.class);

    Function<Field, Object> fieldToObject =
        new Function<>() {
          @Override
          public Object apply(Field field) {
            return "hello";
          }
        };
    fieldToObject = spy(fieldToObject);

    AnnotatedFields.populateAnnotatedFields(
        extensionContext, null, NessieVersion.class, a -> true, fieldToObject);

    soft.assertThat(AnnotatedFieldsTarget.nessieVersionAnnotatedStatic).isEqualTo("hello");
    soft.assertThat(AnnotatedFieldsTarget.nessieApiAnnotatedStatic).isNull();
    verify(fieldToObject)
        .apply(AnnotatedFieldsTarget.class.getDeclaredField("nessieVersionAnnotatedStatic"));
  }

  @Test
  void nessieApiInjectionInstance() throws Exception {
    ExtensionContext extensionContext = mock(ExtensionContext.class);
    when(extensionContext.getRequiredTestClass()).thenReturn((Class) AnnotatedFieldsTarget.class);

    Function<Field, Object> fieldToObject =
        new Function<>() {
          @Override
          public Object apply(Field field) {
            return "hello";
          }
        };
    fieldToObject = spy(fieldToObject);

    AnnotatedFieldsTarget instance = new AnnotatedFieldsTarget();

    AnnotatedFields.populateNessieApiFields(
        extensionContext, instance, TargetVersion.TESTED, fieldToObject);

    soft.assertThat(instance.nessieApiAnnotatedInstance).isEqualTo("hello");
    soft.assertThat(instance.nessieVersionAnnotatedInstance).isNull();
    soft.assertThat(AnnotatedFieldsTarget.nessieApiAnnotatedStatic).isNull();
    soft.assertThat(AnnotatedFieldsTarget.nessieVersionAnnotatedStatic).isNull();
    verify(fieldToObject)
        .apply(AnnotatedFieldsTarget.class.getDeclaredField("nessieApiAnnotatedInstance"));
  }

  @Test
  void genericInjectionInstance() throws Exception {
    ExtensionContext extensionContext = mock(ExtensionContext.class);
    when(extensionContext.getRequiredTestClass()).thenReturn((Class) AnnotatedFieldsTarget.class);

    Function<Field, Object> fieldToObject =
        new Function<>() {
          @Override
          public Object apply(Field field) {
            return "hello";
          }
        };
    fieldToObject = spy(fieldToObject);

    AnnotatedFieldsTarget instance = new AnnotatedFieldsTarget();

    AnnotatedFields.populateAnnotatedFields(
        extensionContext, instance, NessieVersion.class, a -> true, fieldToObject);

    soft.assertThat(instance.nessieApiAnnotatedInstance).isNull();
    soft.assertThat(instance.nessieVersionAnnotatedInstance).isEqualTo("hello");
    soft.assertThat(AnnotatedFieldsTarget.nessieApiAnnotatedStatic).isNull();
    soft.assertThat(AnnotatedFieldsTarget.nessieVersionAnnotatedStatic).isNull();
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
