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
package org.projectnessie.events.ri.messaging;

/** Headers that are added to the outgoing messages. */
public enum MessageHeaders {
  EVENT_ID("event-id"),
  SPEC_VERSION("spec-version"),
  API_VERSION("api-version"),
  REPOSITORY_ID("repository-id"),
  INITIATOR("initiator"),
  EVENT_TYPE("event-type"),
  EVENT_CREATION_TIME("event-creation-time"),
  COMMIT_CREATION_TIME("commit-creation-time"),
  ;

  private final String key;

  MessageHeaders(String key) {
    this.key = key;
  }

  public String key() {
    return key;
  }
}
