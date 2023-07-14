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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.projectnessie.model.RepositoryConfig;

@ExtendWith(SoftAssertionsExtension.class)
public class TestCustomRepositoryConfigType {
  static final ObjectMapper MAPPER = new ObjectMapper();

  @InjectSoftAssertions protected SoftAssertions soft;

  @Test
  void directSerialization() throws Exception {
    CustomTestRepositoryConfig testRepositoryConfig =
        ImmutableCustomTestRepositoryConfig.builder().someLong(42L).someString("blah").build();

    String json = MAPPER.writeValueAsString(testRepositoryConfig);

    RepositoryConfig deserializedAsRepositoryConfig =
        MAPPER.readValue(json, RepositoryConfig.class);
    CustomTestRepositoryConfig deserializedAsTestRepositoryConfig =
        MAPPER.readValue(json, CustomTestRepositoryConfig.class);

    soft.assertThat(deserializedAsRepositoryConfig).isEqualTo(testRepositoryConfig);
    soft.assertThat(deserializedAsTestRepositoryConfig).isEqualTo(testRepositoryConfig);
  }
}
