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
package org.projectnessie.client.auth.oauth2;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class TestJacksonSerializers {

  @Test
  void testSerializeInstantToSeconds() throws JsonProcessingException {
    MyBean input = new MyBean();
    input.setExpiresAt(Instant.now().plusSeconds(100));
    String expected = "\\{\"expiresAt\":\\d+}";
    String actual = new ObjectMapper().writeValueAsString(input);
    assertThat(actual).containsPattern(expected);
  }

  @Test
  void testDeserializeSecondsToInstant() throws JsonProcessingException {
    MyBean expected = new MyBean();
    expected.setExpiresAt(Instant.now().plusSeconds(100));
    String json = "{\"expiresAt\":100}";
    MyBean actual = new ObjectMapper().readValue(json, MyBean.class);
    assertThat(actual.getExpiresAt())
        // allow for a 10-second clock skew
        .isBetween(
            expected.getExpiresAt().minusSeconds(10), expected.getExpiresAt().plusSeconds(10));
  }

  @Test
  void testSerializeNull() throws JsonProcessingException {
    String actual = new ObjectMapper().writeValueAsString(new MyBean());
    assertThat(actual).isEqualTo("{\"expiresAt\":null}");
  }

  @Test
  void testDeserializeNull() throws JsonProcessingException {
    MyBean actual = new ObjectMapper().readValue("{\"expiresAt\":null}", MyBean.class);
    assertThat(actual.getExpiresAt()).isNull();
  }

  private static class MyBean {

    @JsonSerialize(using = JacksonSerializers.InstantToSecondsSerializer.class)
    @JsonDeserialize(using = JacksonSerializers.SecondsToInstantDeserializer.class)
    private Instant expiresAt;

    public Instant getExpiresAt() {
      return expiresAt;
    }

    public void setExpiresAt(Instant expiresAt) {
      this.expiresAt = expiresAt;
    }
  }
}
