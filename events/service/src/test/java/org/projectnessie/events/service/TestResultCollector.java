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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.security.Principal;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.projectnessie.versioned.Result;
import org.projectnessie.versioned.ResultType;

@ExtendWith(MockitoExtension.class)
class TestResultCollector {

  @Mock EventService service;
  @Mock EventSubscribers subscribers;
  @Mock Result result;

  Principal alice = () -> "alice";

  @Test
  void onResultWithSubscribers() {
    when(result.getResultType()).thenReturn(ResultType.COMMIT);
    when(subscribers.hasSubscribersFor(ResultType.COMMIT)).thenReturn(true);
    ResultCollector collector = new ResultCollector(subscribers, "repo1", alice, service);
    collector.accept(result);
    VersionStoreEvent expected =
        ImmutableVersionStoreEvent.builder()
            .result(result)
            .repositoryId("repo1")
            .user(alice)
            .build();
    verify(service).onVersionStoreEvent(expected);
  }

  @Test
  void onResultWithSubscribersNoUser() {
    when(result.getResultType()).thenReturn(ResultType.COMMIT);
    when(subscribers.hasSubscribersFor(ResultType.COMMIT)).thenReturn(true);
    ResultCollector collector = new ResultCollector(subscribers, "repo1", null, service);
    collector.accept(result);
    VersionStoreEvent expected =
        ImmutableVersionStoreEvent.builder().result(result).repositoryId("repo1").build();
    verify(service).onVersionStoreEvent(expected);
  }

  @Test
  void onResultWithoutSubscribers() {
    when(result.getResultType()).thenReturn(ResultType.COMMIT);
    when(subscribers.hasSubscribersFor(ResultType.COMMIT)).thenReturn(false);
    ResultCollector collector = new ResultCollector(subscribers, "repo1", alice, service);
    collector.accept(result);
    verify(service, never()).onVersionStoreEvent(any());
  }
}
