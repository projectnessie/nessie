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
package org.projectnessie.events.quarkus.fixtures;

import io.quarkus.test.Mock;
import javax.enterprise.inject.Produces;
import javax.inject.Singleton;
import org.projectnessie.events.quarkus.QuarkusEventSubscribers;

@Mock
@Singleton
public class MockEventSubscribers extends QuarkusEventSubscribers {

  @Produces
  @Singleton
  public MockEventSubscriber.MockEventSubscriber1 produceSubscriber1() {
    return findSubscriber(MockEventSubscriber.MockEventSubscriber1.class);
  }

  @Produces
  @Singleton
  public MockEventSubscriber.MockEventSubscriber2 produceSubscriber2() {
    return findSubscriber(MockEventSubscriber.MockEventSubscriber2.class);
  }

  private <T extends MockEventSubscriber> T findSubscriber(Class<? extends T> cl) {
    return getSubscribers().stream().filter(cl::isInstance).map(cl::cast).findFirst().orElseThrow();
  }
}
