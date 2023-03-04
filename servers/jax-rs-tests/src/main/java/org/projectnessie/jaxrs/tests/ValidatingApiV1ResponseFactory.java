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
package org.projectnessie.jaxrs.tests;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.List;
import org.projectnessie.client.http.HttpResponse;
import org.projectnessie.client.http.HttpResponseFactory;
import org.projectnessie.client.http.ResponseContext;

public class ValidatingApiV1ResponseFactory implements HttpResponseFactory {

  private static final List<String> API_V2_ATTRIBUTES =
      ImmutableList.of(
          // This list should include at least one v2 element from each payload type that has
          // V2-specific API attributes, but it is not necessary to list all v2 attributes from
          // each type. Assume that v2-specific attributes do not clash with valid v1 attributes.
          "\"contentId\"",
          "\"effectiveReference\"",
          "\"authors\"",
          "\"allSignedOffBy\"",
          "\"addedContents\"",
          "\"effectiveFromReference\"");

  @Override
  public HttpResponse make(ResponseContext context, ObjectMapper mapper) {
    return new HttpResponse(context, mapper) {
      @Override
      public <V> V readEntity(Class<V> clazz) {
        try {
          JsonNode jsonNode = mapper.readValue(context.getInputStream(), JsonNode.class);

          // Make sure API v2 attributes are not present. Newer/current clients would be
          // able to parse this attribute (if preset) using the up-to-date payload
          // class, but older clients would brake if v2 attributes were present in API
          // v1 responses.
          String jsonString = jsonNode.toPrettyString(); // for nicer error message
          assertThat(jsonString).doesNotContain(API_V2_ATTRIBUTES);

          return mapper.readerFor(clazz).readValue(jsonNode);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    };
  }
}
