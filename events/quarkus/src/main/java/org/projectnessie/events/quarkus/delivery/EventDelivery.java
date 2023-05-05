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
package org.projectnessie.events.quarkus.delivery;

/**
 * Interface for encapsulating the event delivery logic of a single event to a single subscriber.
 */
public interface EventDelivery {

  /**
   * Starts the delivery of an event. Called only once per delivery.
   *
   * <p>This method is deliberately returning nothing, since the delivery is designed to be in
   * fire-and-forget mode. It does not throw either when the delivery failed.
   */
  void start();
}
