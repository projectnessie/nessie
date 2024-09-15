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
package org.projectnessie.events.service.util;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.Map;
import org.projectnessie.catalog.model.ops.CatalogOperation;
import org.projectnessie.events.api.Content;
import org.projectnessie.events.api.ContentKey;
import org.projectnessie.events.api.ImmutableContent;
import org.projectnessie.events.api.catalog.ImmutableCatalogOperation;
import org.projectnessie.events.api.catalog.ImmutableCatalogUpdate;

public final class ContentMapping {

  private static final ObjectMapper MAPPER =
      new ObjectMapper().setSerializationInclusion(JsonInclude.Include.NON_NULL);

  private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {};

  private ContentMapping() {}

  public static Content map(org.projectnessie.model.Content content) {
    Map<String, ?> map = MAPPER.convertValue(content, MAP_TYPE);
    return ImmutableContent.builder()
        .id((String) map.remove("id"))
        .type((String) map.remove("type"))
        .putAllProperties(map)
        .build();
  }

  public static ContentKey map(org.projectnessie.model.ContentKey key) {
    return ContentKey.of(key.getElements());
  }

  public static ImmutableCatalogOperation map(CatalogOperation<?> operation) {
    Map<String, ?> operationMap = MAPPER.convertValue(operation, MAP_TYPE);
    operationMap.remove("operationType");
    operationMap.remove("contentKey");
    operationMap.remove("contentType");
    List<?> updates = (List<?>) operationMap.remove("updates");
    ImmutableCatalogOperation.Builder builder =
        ImmutableCatalogOperation.builder().properties(operationMap);
    for (Object update : updates) {
      @SuppressWarnings("unchecked")
      Map<String, ?> updateMap = (Map<String, ?>) update;
      String action = (String) updateMap.remove("action");
      builder.addUpdate(
          ImmutableCatalogUpdate.builder().action(action).properties(updateMap).build());
    }
    return builder.build();
  }
}
