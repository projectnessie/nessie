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
package org.projectnessie.model;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import org.junit.jupiter.api.Test;

public class ContentsKeySerdeTest {

  @Test
  void testNamespaceNotIncluded() throws IOException {
    ObjectMapper mapper = new ObjectMapper();

    Namespace namespace = Namespace.parse("a.b.c.tableName");
    assertThat(namespace.name()).isEqualTo("a.b.c");

    ContentsKey key = ContentsKey.of("a", "b", "c", "tableName");
    assertThat(key.getNamespace()).isEqualTo(namespace);
    String serializedKey = mapper.writeValueAsString(key);
    assertThat(serializedKey).contains("elements").doesNotContain("namespace");

    ContentsKey deserialized = mapper.readValue(serializedKey, ContentsKey.class);
    assertThat(deserialized).isEqualTo(key);
    assertThat(deserialized.getNamespace()).isEqualTo(namespace);
  }
}
