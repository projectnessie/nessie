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
package org.projectnessie.events.spi;

import java.util.UUID;
import org.immutables.value.Value;

/**
 * A subscription to an event.
 *
 * <p>An instance of this class is provided by Nessie to the {@link
 * EventSubscriber#onSubscribe(EventSubscription) onSubscribe} method.
 *
 * @see EventSubscriber#onSubscribe(EventSubscription)
 */
@Value.Immutable
public interface EventSubscription {

  /** The unique identifier of this subscription. */
  UUID getId();

  /** The configuration of the Nessie server that this subscription is connected to. */
  NessieConfiguration getNessieConfiguration();
}
