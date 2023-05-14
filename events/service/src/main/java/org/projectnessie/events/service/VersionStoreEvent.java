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
package org.projectnessie.events.service;

import java.security.Principal;
import java.util.Optional;
import org.immutables.value.Value;
import org.projectnessie.versioned.Result;

/**
 * An internal event triggered when a result produced by the version store.
 *
 * <p>This event is meant to capture the {@link Result} emitted by the store, correlated with the
 * user that initiated the change, and the id of the repository affected by the change.
 *
 * <p>This event is produced by a {@link ResultCollector}; it is not meant to be delivered as is to
 * subscribers, but rather, to be forwarded to {@link
 * EventService#onVersionStoreEvent(VersionStoreEvent)}, then converted to one or many {@linkplain
 * org.projectnessie.events.api.Event API events}, which are in turn delivered to subscribers.
 */
@Value.Immutable
@Value.Style(optionalAcceptNullable = true)
public interface VersionStoreEvent {

  /** The {@link Result} produced by the version store. */
  Result getResult();

  /** The repository id affected by the change. Never null, but may be an empty string. */
  String getRepositoryId();

  /** The user principal that initiated the change. May be empty if authentication is disabled. */
  Optional<Principal> getUser();
}
