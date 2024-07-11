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
package org.projectnessie.gc.contents;

import java.util.UUID;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.projectnessie.gc.contents.inmem.InMemoryPersistenceSpi;
import org.projectnessie.gc.contents.spi.PersistenceSpi;

@ExtendWith(SoftAssertionsExtension.class)
public class TestLiveContentsSetRepository {
  @InjectSoftAssertions SoftAssertions soft;

  @Test
  public void newAddContentsFinishExceptionally() throws Exception {
    PersistenceSpi persistenceSpi = new InMemoryPersistenceSpi();

    LiveContentSetsRepository liveContentSetsRepository =
        LiveContentSetsRepository.builder().persistenceSpi(persistenceSpi).build();
    UUID id;
    try (AddContents addContents = liveContentSetsRepository.newAddContents()) {
      id = addContents.id();
      addContents.finishedExceptionally(new Exception("dummy foo"));
    }

    LiveContentSet liveContentSet = liveContentSetsRepository.getLiveContentSet(id);
    soft.assertThat(liveContentSet.status()).isSameAs(LiveContentSet.Status.IDENTIFY_FAILED);
    soft.assertThat(liveContentSet.errorMessage()).contains("dummy foo");
  }

  @Test
  public void newAddContentsFinishNormally() throws Exception {
    PersistenceSpi persistenceSpi = new InMemoryPersistenceSpi();

    LiveContentSetsRepository liveContentSetsRepository =
        LiveContentSetsRepository.builder().persistenceSpi(persistenceSpi).build();
    UUID id;
    try (AddContents addContents = liveContentSetsRepository.newAddContents()) {
      id = addContents.id();
      addContents.finished();
    }

    LiveContentSet liveContentSet = liveContentSetsRepository.getLiveContentSet(id);
    soft.assertThat(liveContentSet.status()).isSameAs(LiveContentSet.Status.IDENTIFY_SUCCESS);
    soft.assertThat(liveContentSet.errorMessage()).isNull();
  }
}
