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
package org.projectnessie.versioned.storage.versionstore;

import static java.util.Collections.singletonList;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import org.assertj.core.api.SoftAssertions;
import org.assertj.core.api.junit.jupiter.InjectSoftAssertions;
import org.assertj.core.api.junit.jupiter.SoftAssertionsExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.projectnessie.model.CommitMeta;
import org.projectnessie.model.ContentKey;
import org.projectnessie.model.IcebergTable;
import org.projectnessie.model.Operation.Put;
import org.projectnessie.versioned.BranchName;
import org.projectnessie.versioned.CommitResult;
import org.projectnessie.versioned.GetNamedRefsParams;
import org.projectnessie.versioned.ReferenceInfo;
import org.projectnessie.versioned.VersionStore;
import org.projectnessie.versioned.storage.cache.CacheConfig;
import org.projectnessie.versioned.storage.cache.PersistCaches;
import org.projectnessie.versioned.storage.common.persist.Persist;
import org.projectnessie.versioned.storage.testextension.NessiePersist;
import org.projectnessie.versioned.storage.testextension.PersistExtension;

@ExtendWith({PersistExtension.class, SoftAssertionsExtension.class})
public class TestVersionStoreReferenceCaching {
  @InjectSoftAssertions protected SoftAssertions soft;

  Persist wrapWithCache(Persist persist, LongSupplier clockNanos) {
    return PersistCaches.newBackend(
            CacheConfig.builder()
                .capacityMb(32)
                .clockNanos(clockNanos)
                .referenceTtl(Duration.ofMinutes(1))
                .referenceNegativeTtl(Duration.ofSeconds(1))
                .cacheCapacityOvershoot(0.1d)
                .build())
        .wrap(persist);
  }

  // Two caching `Persist` instances, using _independent_ cache backends.
  Persist withCache1;
  Persist withCache2;

  AtomicLong nowNanos;

  protected VersionStore store1;
  protected VersionStore store2;

  @BeforeEach
  void wrapCaches(@NessiePersist Persist persist1, @NessiePersist Persist persist2) {
    nowNanos = new AtomicLong();
    withCache1 = wrapWithCache(persist1, nowNanos::get);
    withCache2 = wrapWithCache(persist2, nowNanos::get);

    store1 = ValidatingVersionStoreImpl.of(soft, withCache1);
    store2 = ValidatingVersionStoreImpl.of(soft, withCache2);
  }

  @Test
  public void referenceCachingConcurrentCommit() throws Exception {
    BranchName main = BranchName.of("main");

    ReferenceInfo<CommitMeta> head1 =
        store1.getNamedRef(main.getName(), GetNamedRefsParams.DEFAULT);
    ReferenceInfo<CommitMeta> head2 =
        store2.getNamedRef(main.getName(), GetNamedRefsParams.DEFAULT);
    soft.assertThat(head2).isEqualTo(head1);

    CommitResult committed1 =
        store1.commit(
            main,
            Optional.of(head1.getHash()),
            CommitMeta.fromMessage("commit via 1"),
            singletonList(Put.of(ContentKey.of("table1"), IcebergTable.of("/foo", 1, 2, 3, 43))));

    soft.assertThat(committed1.getCommit().getParentHash()).isEqualTo(head1.getHash());

    ReferenceInfo<CommitMeta> head1afterCommit1 =
        store1.getNamedRef(main.getName(), GetNamedRefsParams.DEFAULT);
    ReferenceInfo<CommitMeta> head2afterCommit1 =
        store2.getNamedRef(main.getName(), GetNamedRefsParams.DEFAULT);
    soft.assertThat(head1afterCommit1.getHash())
        .isEqualTo(committed1.getCommitHash())
        .isNotEqualTo(head1.getHash());
    soft.assertThat(head2afterCommit1).isEqualTo(head2);

    CommitResult committed2 =
        store2.commit(
            main,
            Optional.of(head2afterCommit1.getHash()),
            CommitMeta.fromMessage("commit via 2"),
            singletonList(Put.of(ContentKey.of("table2"), IcebergTable.of("/foo", 1, 2, 3, 43))));

    soft.assertThat(committed2.getCommit().getParentHash())
        .isEqualTo(committed1.getCommit().getHash());

    ReferenceInfo<CommitMeta> head1afterCommit2 =
        store1.getNamedRef(main.getName(), GetNamedRefsParams.DEFAULT);
    ReferenceInfo<CommitMeta> head2afterCommit2 =
        store2.getNamedRef(main.getName(), GetNamedRefsParams.DEFAULT);
    soft.assertThat(head1afterCommit2).isEqualTo(head1afterCommit1);
    soft.assertThat(head2afterCommit2.getHash())
        .isEqualTo(committed2.getCommitHash())
        .isNotEqualTo(head1afterCommit2.getHash());

    //

    nowNanos.addAndGet(Duration.ofMinutes(5).toNanos());

    ReferenceInfo<CommitMeta> head1afterExpiry =
        store1.getNamedRef(main.getName(), GetNamedRefsParams.DEFAULT);
    ReferenceInfo<CommitMeta> head2afterExpiry =
        store2.getNamedRef(main.getName(), GetNamedRefsParams.DEFAULT);

    soft.assertThat(head1afterExpiry).isEqualTo(head2afterExpiry).isEqualTo(head2afterCommit2);
  }
}
