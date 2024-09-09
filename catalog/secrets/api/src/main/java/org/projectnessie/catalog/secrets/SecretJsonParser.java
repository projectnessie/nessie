/*
 * Copyright (C) 2024 Dremio
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
package org.projectnessie.catalog.secrets;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;

final class SecretJsonParser {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private SecretJsonParser() {}

  @SuppressWarnings("unchecked")
  static Map<String, String> parseOrSingle(String s) {
    if (!s.trim().startsWith("{")) {
      return Map.of("value", s);
    }
    try {
      return MAPPER.readValue(s, Map.class);
    } catch (JsonProcessingException e) {
      return Map.of("value", s);
    }
  }
}
