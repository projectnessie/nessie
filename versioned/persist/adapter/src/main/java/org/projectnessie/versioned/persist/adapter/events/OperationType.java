/*
 * Copyright (C) 2022 Dremio
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
package org.projectnessie.versioned.persist.adapter.events;

public enum OperationType {
  COMMIT(true, false),
  TRANSPLANT(true, false),
  MERGE(true, false),
  CRETE_REF(false, true),
  DELETE_REF(false, true),
  ASSIGN_REF(false, true),
  REPOSITORY_INITIALIZED(false, false),
  REPOSITORY_ERASED(false, false);

  private final boolean committingEvent;
  private final boolean reference;

  OperationType(boolean committing, boolean reference) {
    this.committingEvent = committing;
    this.reference = reference;
  }

  /** Whether the event / operation-type is one that wrote (at least) one commit. */
  public boolean isCommitting() {
    return committingEvent;
  }

  /** Whether the event / operation-type is one that modified a named reference. */
  public boolean isReference() {
    return reference;
  }
}
