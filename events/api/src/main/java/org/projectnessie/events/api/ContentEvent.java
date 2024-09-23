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
package org.projectnessie.events.api;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.time.Instant;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.ContentKey;

/**
 * Event that is emitted when a content is stored (PUT) or removed (DELETE).
 *
 * <p>This type has 2 child interfaces:
 *
 * <ul>
 *   <li>{@link ContentStoredEvent}: for PUT operations;
 *   <li>{@link ContentRemovedEvent}: for DELETE operations.
 * </ul>
 */
public interface ContentEvent extends ReferenceEvent {

  /** The hash of the commit that the content was stored in or removed from. */
  String getHash();

  /** The timestamp of the commit that the content was stored in or removed from. */
  @JsonSerialize(using = CommitMeta.InstantSerializer.class)
  @JsonDeserialize(using = CommitMeta.InstantDeserializer.class)
  Instant getCommitCreationTimestamp();

  /** The key of the content that was stored or removed. */
  ContentKey getContentKey();
}
