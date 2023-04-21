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

import com.google.errorprone.annotations.CanIgnoreReturnValue;

/**
 * Event that is emitted when a content is stored (PUT) or removed (DELETE).
 *
 * <p>There are two concrete implementations of this interface:
 *
 * <ul>
 *   <li>{@link ContentStoredEvent}: for PUT operations;
 *   <li>{@link ContentRemovedEvent}: for DELETE operations.
 * </ul>
 */
public interface ContentEvent extends Event {

  /** The branch that the content was stored in or removed from. */
  String getBranch();

  /** The hash of the commit that the content was stored in or removed from. */
  String getHash();

  /** The key of the content that was stored or removed. */
  ContentKey getContentKey();

  interface Builder<B extends Builder<B, E>, E extends ContentEvent> extends Event.Builder<B, E> {

    @CanIgnoreReturnValue
    B branch(String branchName);

    @CanIgnoreReturnValue
    B hash(String hash);

    @CanIgnoreReturnValue
    B contentKey(ContentKey key);
  }
}
