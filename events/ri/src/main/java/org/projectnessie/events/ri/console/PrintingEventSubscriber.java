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
package org.projectnessie.events.ri.console;

import jakarta.enterprise.context.ApplicationScoped;
import java.io.PrintStream;
import org.projectnessie.events.api.CommitEvent;
import org.projectnessie.events.api.ContentRemovedEvent;
import org.projectnessie.events.api.ContentStoredEvent;
import org.projectnessie.events.api.MergeEvent;
import org.projectnessie.events.api.ReferenceCreatedEvent;
import org.projectnessie.events.api.ReferenceDeletedEvent;
import org.projectnessie.events.api.ReferenceUpdatedEvent;
import org.projectnessie.events.api.TransplantEvent;
import org.projectnessie.events.spi.EventSubscriber;
import org.projectnessie.events.spi.EventSubscription;

/**
 * A simple {@link EventSubscriber} that prints all events to a {@link PrintStream}, stdout by
 * default.
 *
 * <p>Do NOT use this in production, it would spam your console!
 */
@ApplicationScoped
public class PrintingEventSubscriber implements EventSubscriber {

  private final PrintStream out;

  @SuppressWarnings("unused")
  public PrintingEventSubscriber() {
    this(System.out);
  }

  public PrintingEventSubscriber(PrintStream out) {
    this.out = out;
  }

  @Override
  public void onSubscribe(EventSubscription subscription) {
    out.println("Subscription: " + subscription);
  }

  @Override
  public void onReferenceCreated(ReferenceCreatedEvent event) {
    out.println("Reference created: " + event);
  }

  @Override
  public void onReferenceUpdated(ReferenceUpdatedEvent event) {
    out.println("Reference updated: " + event);
  }

  @Override
  public void onReferenceDeleted(ReferenceDeletedEvent event) {
    out.println("Reference deleted: " + event);
  }

  @Override
  public void onCommit(CommitEvent event) {
    out.println("Commit: " + event);
  }

  @Override
  public void onMerge(MergeEvent event) {
    out.println("Merge: " + event);
  }

  @Override
  public void onTransplant(TransplantEvent event) {
    out.println("Transplant: " + event);
  }

  @Override
  public void onContentStored(ContentStoredEvent event) {
    out.println("Content stored: " + event);
  }

  @Override
  public void onContentRemoved(ContentRemovedEvent event) {
    out.println("Content removed: " + event);
  }

  @Override
  public void close() {
    out.println("closed");
  }
}
