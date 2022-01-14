/*
 * Copyright (C) 2020 Dremio
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
package org.projectnessie.test.compatibility;

import static java.lang.reflect.Modifier.PROTECTED;
import static java.lang.reflect.Modifier.PUBLIC;
import static java.lang.reflect.Modifier.STATIC;
import static org.assertj.core.api.Assertions.assertThat;
import static org.projectnessie.test.compatibility.Util.CLS_NAME_ABSTRACT_TEST_REST;
import static org.projectnessie.test.compatibility.Util.CLS_NAME_ABSTRACT_TEST_RESTEASY;
import static org.projectnessie.test.compatibility.Util.CLS_NAME_REST_ASSURED;
import static org.projectnessie.test.compatibility.Util.FIELD_NESSIE_VERSION;
import static org.projectnessie.test.compatibility.Util.METHOD_UPDATE_TEST_SERVER_URI;

import java.net.URI;
import java.util.Collection;
import org.junit.jupiter.api.Test;

public class TestValidateTestClasses {
  @Test
  void validateTestClassesApis() throws Exception {
    URI uri = URI.create("http://localhost:42666/api/v13");
    URI otherUri = URI.create("http://localhost:12345/api/v42");

    String versionInClass = "1.2.3";
    Collection<Class<?>> testClasses =
        TestClassesGenerator.resolveTestClasses("0.17.0", versionInClass, "0_17_0");

    assertThat(testClasses)
        .hasSize(2)
        .anySatisfy(
            c -> assertThat(c.getSuperclass().getName()).isEqualTo(CLS_NAME_ABSTRACT_TEST_REST))
        .anySatisfy(
            c ->
                assertThat(c.getSuperclass().getName()).isEqualTo(CLS_NAME_ABSTRACT_TEST_RESTEASY));

    Class<?> testClassRest =
        testClasses.stream()
            .filter(c -> c.getSuperclass().getName().equals(CLS_NAME_ABSTRACT_TEST_REST))
            .findFirst()
            .get();
    Class<?> testClassResteasy =
        testClasses.stream()
            .filter(c -> c.getSuperclass().getName().equals(CLS_NAME_ABSTRACT_TEST_RESTEASY))
            .findFirst()
            .get();

    // Must succeed
    testClassResteasy.getDeclaredConstructor().newInstance();

    assertThat(testClassResteasy.getDeclaredField("testUri"))
        .satisfies(f -> assertThat(f.getModifiers()).isEqualTo(PUBLIC | STATIC))
        .satisfies(f -> assertThat(f.getType()).isSameAs(URI.class))
        .satisfies(f -> assertThat(f.get(null)).isNull());
    assertThat(testClassResteasy.getSuperclass().getDeclaredField("basePath"))
        .satisfies(f -> assertThat(f.getModifiers()).isEqualTo(PROTECTED | STATIC))
        .satisfies(f -> assertThat(f.getType()).isSameAs(String.class));
    assertThat(testClassResteasy.getDeclaredMethod(METHOD_UPDATE_TEST_SERVER_URI, URI.class))
        .satisfies(m -> assertThat(m.getModifiers()).isEqualTo(PUBLIC | STATIC))
        .satisfies(m -> assertThat(m.getParameterTypes()).containsExactly(URI.class))
        .satisfies(m -> assertThat(m.getReturnType()).isSameAs(void.class));
    assertThat(testClassResteasy.getDeclaredField(FIELD_NESSIE_VERSION))
        .satisfies(f -> assertThat(f.getModifiers()).isEqualTo(PUBLIC | STATIC))
        .satisfies(f -> assertThat(f.getType()).isSameAs(String.class))
        .satisfies(f -> assertThat(f.get(null)).isSameAs(versionInClass));

    // try updating
    testClassResteasy.getDeclaredMethod(METHOD_UPDATE_TEST_SERVER_URI, URI.class).invoke(null, uri);
    assertThat(testClassResteasy.getDeclaredField("testUri").get(null)).isEqualTo(uri);

    Class<?> clsRestAssured = testClassResteasy.getClassLoader().loadClass(CLS_NAME_REST_ASSURED);
    assertThat(clsRestAssured.getDeclaredField("port").get(null)).isEqualTo(uri.getPort());
    assertThat(clsRestAssured.getDeclaredField("basePath").get(null)).isEqualTo(uri.getPath());
    assertThat(clsRestAssured.getDeclaredField("baseURI").get(null))
        .isEqualTo(String.format("%s://%s", uri.getScheme(), uri.getHost()));

    // try updating
    testClassResteasy
        .getDeclaredMethod(METHOD_UPDATE_TEST_SERVER_URI, URI.class)
        .invoke(null, otherUri);
    assertThat(testClassResteasy.getDeclaredField("testUri").get(null)).isEqualTo(otherUri);

    assertThat(clsRestAssured.getDeclaredField("port").get(null)).isEqualTo(otherUri.getPort());
    assertThat(clsRestAssured.getDeclaredField("basePath").get(null)).isEqualTo(otherUri.getPath());
    assertThat(clsRestAssured.getDeclaredField("baseURI").get(null))
        .isEqualTo(String.format("%s://%s", otherUri.getScheme(), otherUri.getHost()));

    // Must succeed
    testClassRest.getDeclaredConstructor().newInstance();

    assertThat(testClassRest.getDeclaredField("testUri"))
        .satisfies(f -> assertThat(f.getModifiers()).isEqualTo(PUBLIC | STATIC))
        .satisfies(f -> assertThat(f.getType()).isSameAs(URI.class))
        .satisfies(f -> assertThat(f.get(null)).isNull());
    assertThat(testClassRest.getDeclaredMethod("setUp"))
        .satisfies(m -> assertThat(m.getModifiers()).isEqualTo(PUBLIC))
        .satisfies(m -> assertThat(m.getParameterCount()).isEqualTo(0))
        .satisfies(m -> assertThat(m.getReturnType()).isSameAs(void.class));
    assertThat(testClassRest.getDeclaredMethod(METHOD_UPDATE_TEST_SERVER_URI, URI.class))
        .satisfies(m -> assertThat(m.getModifiers()).isEqualTo(PUBLIC | STATIC))
        .satisfies(m -> assertThat(m.getParameterTypes()).containsExactly(URI.class))
        .satisfies(m -> assertThat(m.getReturnType()).isSameAs(void.class));
    assertThat(testClassRest.getDeclaredField(FIELD_NESSIE_VERSION))
        .satisfies(f -> assertThat(f.getModifiers()).isEqualTo(PUBLIC | STATIC))
        .satisfies(f -> assertThat(f.getType()).isSameAs(String.class))
        .satisfies(f -> assertThat(f.get(null)).isSameAs(versionInClass));

    // try updating
    testClassRest.getDeclaredMethod(METHOD_UPDATE_TEST_SERVER_URI, URI.class).invoke(null, uri);
    assertThat(testClassRest.getDeclaredField("testUri").get(null)).isEqualTo(uri);

    // try updating
    testClassRest
        .getDeclaredMethod(METHOD_UPDATE_TEST_SERVER_URI, URI.class)
        .invoke(null, otherUri);
    assertThat(testClassRest.getDeclaredField("testUri").get(null)).isEqualTo(otherUri);
  }
}
