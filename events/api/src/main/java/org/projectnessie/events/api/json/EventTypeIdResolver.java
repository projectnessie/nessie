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
import org.projectnessie.events.api.CustomEvent;
import org.projectnessie.events.api.Event;
import org.projectnessie.events.api.EventType;

public final class EventTypeIdResolver extends AbstractEnumBasedTypeIdResolver {

  @Override
  public String idFromValue(Object value) {
    return ((Event) value).getType().name();
  }

  @Override
  protected Class<?> subtypeFromId(String id) {
    return EventType.valueOf(id.toUpperCase(Locale.ROOT)).getSubtype();
  }

  @Override
  protected Class<?> customSubtype() {
    return CustomEvent.class;
  }
}
