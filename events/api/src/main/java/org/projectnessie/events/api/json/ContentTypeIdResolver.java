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
package org.projectnessie.events.api.json;

import java.util.Locale;
import org.projectnessie.events.api.Content;
import org.projectnessie.events.api.ContentType;

public final class ContentTypeIdResolver extends AbstractTypeIdResolver {

  @Override
  public String idFromValue(Object value) {
    if (value instanceof Content) {
      return ((Content) value).getType().name();
    }
    return null;
  }

  @Override
  protected Class<?> getSubtype(String id) {
    try {
      return ContentType.valueOf(id.toUpperCase(Locale.ROOT)).getSubtype();
    } catch (Exception e) {
      return ContentType.CUSTOM.getSubtype();
    }
  }
}
