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
package org.projectnessie.events.spi;

import org.projectnessie.events.api.CommitEvent;
import org.projectnessie.events.api.ContentRemovedEvent;
import org.projectnessie.events.api.ContentStoredEvent;
import org.projectnessie.events.api.Event;
import org.projectnessie.events.api.EventType;
import org.projectnessie.events.api.MergeEvent;
import org.projectnessie.events.api.ReferenceCreatedEvent;
import org.projectnessie.events.api.ReferenceDeletedEvent;
import org.projectnessie.events.api.ReferenceUpdatedEvent;
import org.projectnessie.events.api.TransplantEvent;

public class DelegatingEventSubscriber implements EventSubscriber {

  private final EventSubscriber delegate;

  public DelegatingEventSubscriber(EventSubscriber delegate) {
    this.delegate = delegate;
  }

  @Override
  public boolean isBlocking() {
    return delegate.isBlocking();
  }

  @Override
  public void onSubscribe(EventSubscription subscription) {
    delegate.onSubscribe(subscription);
  }

  @Override
  public EventTypeFilter getEventTypeFilter() {
    return delegate.getEventTypeFilter();
  }

  @Override
  public EventFilter getEventFilter() {
    return delegate.getEventFilter();
  }

  @Override
  public boolean accepts(EventType eventType) {
    return delegate.accepts(eventType);
  }

  @Override
  public boolean accepts(Event event) {
    return delegate.accepts(event);
  }

  @Override
  public void onEvent(Event event) {
    delegate.onEvent(event);
  }

  @Override
  public void onReferenceCreated(ReferenceCreatedEvent event) {
    delegate.onReferenceCreated(event);
  }

  @Override
  public void onReferenceUpdated(ReferenceUpdatedEvent event) {
    delegate.onReferenceUpdated(event);
  }

  @Override
  public void onReferenceDeleted(ReferenceDeletedEvent event) {
    delegate.onReferenceDeleted(event);
  }

  @Override
  public void onCommit(CommitEvent event) {
    delegate.onCommit(event);
  }

  @Override
  public void onMerge(MergeEvent event) {
    delegate.onMerge(event);
  }

  @Override
  public void onTransplant(TransplantEvent event) {
    delegate.onTransplant(event);
  }

  @Override
  public void onContentStored(ContentStoredEvent event) {
    delegate.onContentStored(event);
  }

  @Override
  public void onContentRemoved(ContentRemovedEvent event) {
    delegate.onContentRemoved(event);
  }

  @Override
  public void close() throws Exception {
    delegate.close();
  }
}
