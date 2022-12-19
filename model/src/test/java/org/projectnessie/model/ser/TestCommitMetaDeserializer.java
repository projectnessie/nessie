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
package org.projectnessie.model.ser;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import java.util.Arrays;
import java.util.Collections;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.projectnessie.api.v1.ApiAttributesV1;
import org.projectnessie.api.v2.ApiAttributesV2;
import org.projectnessie.model.CommitMeta;

class TestCommitMetaDeserializer {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private CommitMeta deser(Class<?> view, CommitMeta value) throws JsonProcessingException {
    ObjectWriter writer = MAPPER.writer();
    if (view != Object.class) {
      writer = writer.withView(view);
    }
    return MAPPER.readValue(writer.writeValueAsString(value), CommitMeta.class);
  }

  @ParameterizedTest
  @ValueSource(classes = {Object.class, ApiAttributesV1.class, ApiAttributesV2.class})
  void testAuthor(Class<?> view) throws JsonProcessingException {
    CommitMeta meta =
        deser(view, CommitMeta.builder().message("m").author("t1").author("t2").build());
    assertThat(meta.getAuthor()).isEqualTo("t1");
  }

  @ParameterizedTest
  @ValueSource(classes = {Object.class, ApiAttributesV2.class})
  void testAllAuthors(Class<?> view) throws JsonProcessingException {
    CommitMeta meta =
        deser(
            view, CommitMeta.builder().message("m").author("t1").author(null).author("t2").build());
    assertThat(meta.getAllAuthors()).containsExactly("t1", "t2");
  }

  @Test
  void testAllAuthorsTruncated() throws JsonProcessingException {
    CommitMeta meta =
        deser(
            ApiAttributesV1.class,
            CommitMeta.builder().message("m").author("t1").author("t2").build());
    assertThat(meta.getAllAuthors()).containsExactly("t1");
  }

  @ParameterizedTest
  @ValueSource(classes = {Object.class, ApiAttributesV1.class, ApiAttributesV2.class})
  void testSignedOffBy(Class<?> view) throws JsonProcessingException {
    CommitMeta meta =
        deser(
            view,
            CommitMeta.builder()
                .message("m")
                .signedOffBy("s1")
                .signedOffBy(null)
                .signedOffBy("s2")
                .build());
    assertThat(meta.getSignedOffBy()).isEqualTo("s1");
  }

  @ParameterizedTest
  @ValueSource(classes = {Object.class, ApiAttributesV2.class})
  void testAllSignedOffBy(Class<?> view) throws JsonProcessingException {
    CommitMeta meta =
        deser(view, CommitMeta.builder().message("m").signedOffBy("s1").signedOffBy("s2").build());
    assertThat(meta.getAllSignedOffBy()).containsExactly("s1", "s2");
  }

  @ParameterizedTest
  @ValueSource(classes = {Object.class, ApiAttributesV1.class, ApiAttributesV2.class})
  void testSimpleProperties(Class<?> view) throws JsonProcessingException {
    CommitMeta meta =
        deser(
            view,
            CommitMeta.builder()
                .message("m")
                .putAllProperties("k0", Collections.singletonList("overwritten"))
                .properties(Collections.singletonMap("k1", "overwritten"))
                .putAllProperties(Collections.singletonMap("k1", "v1"))
                .putProperties("k2", "v2")
                .build());
    assertThat(meta.getProperties()).hasSize(2).extracting("k1", "k2").containsExactly("v1", "v2");
  }

  @ParameterizedTest
  @ValueSource(classes = {Object.class, ApiAttributesV2.class})
  void testListProperties(Class<?> view) throws JsonProcessingException {
    CommitMeta meta =
        deser(
            view,
            CommitMeta.builder()
                .message("m")
                .putAllProperties(Collections.singletonMap("k1", "v1"))
                .putProperties("k2", "v2")
                .putAllProperties("k3", Collections.singletonList("v3"))
                .putAllProperties("k4", Arrays.asList("v4a", "v4b"))
                .build());
    assertThat(meta.getProperties())
        .hasSize(4)
        .extracting("k1", "k2", "k3", "k4")
        .containsExactly("v1", "v2", "v3", "v4a");
    assertThat(meta.getAllProperties())
        .hasSize(4)
        .extracting("k1", "k2", "k3", "k4")
        .containsExactly(
            Collections.singletonList("v1"),
            Collections.singletonList("v2"),
            Collections.singletonList("v3"),
            Arrays.asList("v4a", "v4b"));
  }

  @Test
  void testListPropertiesTruncated() throws JsonProcessingException {
    CommitMeta meta =
        deser(
            ApiAttributesV1.class,
            CommitMeta.builder()
                .message("m")
                .putAllProperties("k1", Arrays.asList("v1a", "v1b"))
                .build());
    assertThat(meta.getProperties()).isEqualTo(Collections.singletonMap("k1", "v1a"));
    assertThat(meta.getAllProperties())
        .isEqualTo(Collections.singletonMap("k1", Collections.singletonList("v1a")));
  }
}
