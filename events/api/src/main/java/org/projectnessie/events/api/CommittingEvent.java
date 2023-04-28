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

/**
 * Event that is emitted when a transaction is written to the catalog.
 *
 * <p>This type has 3 child interfaces:
 *
 * <ul>
 *   <li>{@link CommitEvent}: for commits;
 *   <li>{@link MergeEvent}: for merges;
 *   <li>{@link TransplantEvent}: for transplants.
 * </ul>
 */
public interface CommittingEvent extends Event {

  /**
   * The source reference where the committed operations came from. This is usually a branch, but
   * not always (e.g. it could be a tag or a detached reference).
   *
   * <p>For commits, this is always the same as the {@linkplain #getTargetReference() target
   * reference}.
   */
  Reference getSourceReference();

  /**
   * The target reference where the committed operations were applied to.
   *
   * <p>For the time being, the target reference is guaranteed to be a branch.
   */
  Reference getTargetReference();

  /** The hash on the {@linkplain #getTargetReference() target reference} before the event. */
  String getHashBefore();

  /** The hash on the {@linkplain #getTargetReference() target reference} after the event. */
  String getHashAfter();
}
