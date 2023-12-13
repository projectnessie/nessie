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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.errorprone.annotations.MustBeClosed;
import jakarta.validation.constraints.NotNull;
import java.time.Clock;
import java.time.Instant;
import java.util.UUID;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.immutables.value.Value;
import org.projectnessie.gc.contents.spi.PersistenceSpi;
import org.projectnessie.gc.identify.IdentifyLiveContents;

/**
 * Binds information about contents, allows unordered addition of content information and grouped
 * retrieval per content.
 *
 * <p>In particular:
 *
 * <ul>
 *   <li>Retrieval whether a particular content-id is still "live", whether any live Nessie commit
 *       references a particular content-id. This handles the case when a content has been deleted
 *       and its data storage can eventually be completely removed.
 *   <li>Addition of <em>base locations</em> per content-id and retrieval of all known <em>base
 *       locations</em> by content-id.
 *   <li>Inventory of {@link IdentifyLiveContents#identifyLiveContents() identify runs} and
 *       information about the collected contents, storing the collected live content in an
 *       unordered way, retrieval of the collected live content per content-id to {@link
 *       org.projectnessie.gc.expire.Expire} implementations.
 * </ul>
 */
@Value.Immutable
public abstract class LiveContentSetsRepository {

  public static Builder builder() {
    return ImmutableLiveContentSetsRepository.builder();
  }

  public interface Builder {
    @CanIgnoreReturnValue
    Builder persistenceSpi(PersistenceSpi persistenceSpi);

    LiveContentSetsRepository build();
  }

  /**
   * Retrieve an existing live-content-set, created via a previous invocation of {@link
   * #newAddContents()}.
   */
  public LiveContentSet getLiveContentSet(UUID id) throws LiveContentSetNotFoundException {
    return persistenceSpi().getLiveContentSet(id);
  }

  @MustBeClosed
  public Stream<LiveContentSet> getAllLiveContents() {
    return persistenceSpi().getAllLiveContents();
  }

  /**
   * Provides the interface used by {@link IdentifyLiveContents} to persist information about live
   * content.
   */
  @MustBeClosed
  public AddContents newAddContents() {
    return new AddContents() {
      private final UUID id = idGenerator().get();
      private final Instant created = clock().instant();
      private volatile boolean closed;
      private boolean finished;
      private Throwable failure;

      {
        persistenceSpi().startIdentifyLiveContents(id, created);
      }

      @Override
      public UUID id() {
        return id;
      }

      @Override
      public Instant created() {
        return created;
      }

      @Override
      public void finished() {
        Preconditions.checkState(!finished, "AddContents instance is already in finished state.");
        finished = true;
      }

      @Override
      public void finishedExceptionally(@NotNull Throwable e) {
        if (failure == null) {
          failure = e;
        } else {
          failure.addSuppressed(e);
        }
        finished = true;
      }

      @Override
      public long addLiveContent(@NotNull Stream<ContentReference> contentReference) {
        Preconditions.checkState(!closed, "AddContents instance already closed.");
        return persistenceSpi().addIdentifiedLiveContent(id, contentReference);
      }

      @Override
      public void close() {
        if (!finished) {
          failure = new IllegalStateException("AddContents instance has not been finished.");
        }
        if (closed) {
          return;
        }
        closed = true;
        persistenceSpi().finishedIdentifyLiveContents(id, clock().instant(), failure);
      }
    };
  }

  abstract PersistenceSpi persistenceSpi();

  @Value.Default
  @VisibleForTesting
  Clock clock() {
    return Clock.systemUTC();
  }

  @Value.Default
  @VisibleForTesting
  Supplier<UUID> idGenerator() {
    return UUID::randomUUID;
  }
}
