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
package org.projectnessie.versioned.tests;

import static java.util.Collections.singleton;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.projectnessie.model.GarbageCollectorConfig;
import org.projectnessie.model.ImmutableGarbageCollectorConfig;
import org.projectnessie.model.RepositoryConfig;
import org.projectnessie.versioned.VersionStore;

@SuppressWarnings("unused")
@ExtendWith(SoftAssertionsExtension.class)
public abstract class AbstractRepositoryConfig extends AbstractNestedVersionStore {
  @InjectSoftAssertions protected SoftAssertions soft;

  protected AbstractRepositoryConfig(VersionStore store) {
    super(store);
  }

  @Test
  void createAndUpdate() throws Exception {
    ImmutableGarbageCollectorConfig created =
        GarbageCollectorConfig.builder()
            .defaultCutoffPolicy("P30D")
            .newFilesGracePeriod(Duration.of(3, ChronoUnit.HOURS))
            .build();
    ImmutableGarbageCollectorConfig updated =
        GarbageCollectorConfig.builder()
            .defaultCutoffPolicy("P10D")
            .expectedFileCountPerContent(123)
            .build();

    soft.assertThat(created.getType()).isEqualTo(RepositoryConfig.Type.GARBAGE_COLLECTOR);
    soft.assertThat(updated.getType()).isEqualTo(RepositoryConfig.Type.GARBAGE_COLLECTOR);

    soft.assertThat(store.getRepositoryConfig(singleton(RepositoryConfig.Type.GARBAGE_COLLECTOR)))
        .isEmpty();

    soft.assertThat(store.updateRepositoryConfig(created)).isNull();

    soft.assertThat(store.getRepositoryConfig(singleton(RepositoryConfig.Type.GARBAGE_COLLECTOR)))
        .containsExactly(created);

    soft.assertThat(store.updateRepositoryConfig(updated)).isEqualTo(created);

    soft.assertThat(store.getRepositoryConfig(singleton(RepositoryConfig.Type.GARBAGE_COLLECTOR)))
        .containsExactly(updated);
  }
}
