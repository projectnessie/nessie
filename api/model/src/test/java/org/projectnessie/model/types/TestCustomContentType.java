/*
 * Copyright (C) 2023 Dremio
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
package org.projectnessie.model.types;

import static org.projectnessie.model.CommitMeta.fromMessage;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.projectnessie.model.Content;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.ImmutableOperations;
import org.projectnessie.model.Operation;
import org.projectnessie.model.Operations;

@ExtendWith(SoftAssertionsExtension.class)
public class TestCustomContentType {
  static final ObjectMapper MAPPER = new ObjectMapper();

  @InjectSoftAssertions protected SoftAssertions soft;

  @Test
  void directSerialization() throws Exception {
    CustomTestContent testContent =
        ImmutableCustomTestContent.builder().someLong(42L).someString("blah").build();

    String json = MAPPER.writeValueAsString(testContent);

    Content deserializedAsContent = MAPPER.readValue(json, Content.class);
    CustomTestContent deserializedAsTestContent = MAPPER.readValue(json, CustomTestContent.class);

    soft.assertThat(deserializedAsContent).isEqualTo(testContent);
    soft.assertThat(deserializedAsTestContent).isEqualTo(testContent);
  }

  @Test
  void customContentInOperation() throws Exception {
    CustomTestContent testContent =
        ImmutableCustomTestContent.builder().someLong(42L).someString("blah").build();

    Operations operations =
        ImmutableOperations.builder()
            .commitMeta(fromMessage("foo"))
            .addOperations(Operation.Put.of(ContentKey.of("key"), testContent))
            .build();

    String json = MAPPER.writeValueAsString(operations);

    Operations deserializedOperations = MAPPER.readValue(json, Operations.class);

    soft.assertThat(deserializedOperations).isEqualTo(operations);
  }
}
