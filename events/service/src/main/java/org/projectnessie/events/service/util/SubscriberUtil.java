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

import org.projectnessie.events.api.CommitEvent;
import org.projectnessie.events.api.ContentRemovedEvent;
import org.projectnessie.events.api.ContentStoredEvent;
import org.projectnessie.events.api.Event;
import org.projectnessie.events.api.MergeEvent;
import org.projectnessie.events.api.ReferenceCreatedEvent;
import org.projectnessie.events.api.ReferenceDeletedEvent;
import org.projectnessie.events.api.ReferenceUpdatedEvent;
import org.projectnessie.events.api.TransplantEvent;
import org.projectnessie.events.spi.EventSubscriber;

public class SubscriberUtil {

  public static void notifySubscriber(EventSubscriber subscriber, Event event) {
    switch (event.getType()) {
      case COMMIT:
        subscriber.onCommit((CommitEvent) event);
        break;
      case MERGE:
        subscriber.onMerge((MergeEvent) event);
        break;
      case TRANSPLANT:
        subscriber.onTransplant((TransplantEvent) event);
        break;
      case REFERENCE_CREATED:
        subscriber.onReferenceCreated((ReferenceCreatedEvent) event);
        break;
      case REFERENCE_UPDATED:
        subscriber.onReferenceUpdated((ReferenceUpdatedEvent) event);
        break;
      case REFERENCE_DELETED:
        subscriber.onReferenceDeleted((ReferenceDeletedEvent) event);
        break;
      case CONTENT_STORED:
        subscriber.onContentStored((ContentStoredEvent) event);
        break;
      case CONTENT_REMOVED:
        subscriber.onContentRemoved((ContentRemovedEvent) event);
        break;
      default:
        throw new IllegalArgumentException("Unknown event type: " + event.getType());
    }
  }
}
