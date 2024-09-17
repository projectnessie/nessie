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
package org.projectnessie.events.api.catalog;

import org.projectnessie.events.api.ContentKey;
import org.projectnessie.events.api.Event;
import org.projectnessie.events.api.ReferenceEvent;

public interface CatalogEvent extends Event, ReferenceEvent {

  /** The key of the content that was updated. */
  ContentKey getContentKey();

  /** The operation that was performed on the content. */
  CatalogOperation getOperation();

  /** The hash of the commit that recorded the catalog operation. */
  String getHash();
}
