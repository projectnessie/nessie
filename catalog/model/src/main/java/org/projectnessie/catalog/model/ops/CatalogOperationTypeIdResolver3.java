/*
 * Copyright (C) 2026 Dremio
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
package org.projectnessie.catalog.model.ops;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;
import org.projectnessie.model.Content;
import org.projectnessie.model.types.ContentTypes;
import tools.jackson.databind.DatabindContext;
import tools.jackson.databind.JavaType;
import tools.jackson.databind.jsontype.impl.TypeIdResolverBase;

/** Jackson 3 resolver for JSON type names to type-specific {@link CatalogOperation} subclasses. */
public class CatalogOperationTypeIdResolver3 extends TypeIdResolverBase {

  private static final List<CatalogOperationTypeResolver> resolvers = load();

  private static List<CatalogOperationTypeResolver> load() {
    ArrayList<CatalogOperationTypeResolver> list = new ArrayList<>();
    ServiceLoader.load(CatalogOperationTypeResolver.class).forEach(list::add);
    return list;
  }

  @Override
  public String idFromValue(DatabindContext context, Object value) {
    if (value instanceof CatalogOperation catalogOperation) {
      return catalogOperation.getType().name();
    }

    return null;
  }

  @Override
  public String idFromValueAndType(DatabindContext context, Object value, Class<?> suggestedType) {
    return idFromValue(context, value);
  }

  @Override
  public JsonTypeInfo.Id getMechanism() {
    return JsonTypeInfo.Id.CUSTOM;
  }

  @Override
  public JavaType typeFromId(DatabindContext context, String id) {
    Content.Type type = ContentTypes.forName(id);
    for (CatalogOperationTypeResolver resolver : resolvers) {
      Class<? extends CatalogOperation> clazz = resolver.forContentType(type);
      if (clazz != null) {
        return context.constructType(clazz);
      }
    }

    throw new IllegalArgumentException("Unresolvable operation type: " + id);
  }
}
