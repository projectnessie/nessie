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

import jakarta.validation.constraints.NotNull;
import java.time.Instant;
import java.util.UUID;
import java.util.stream.Stream;
import org.projectnessie.gc.identify.IdentifyLiveContents;

/**
 * Used by {@link IdentifyLiveContents} to push identified live contents and produce a set of
 * live-content.
 *
 * <p>Instances of this interface are obtained from {@link
 * LiveContentSetsRepository#newAddContents()}.
 *
 * <p>An identify run pushes all identified live contents to {@link #addLiveContent(Stream)} and
 * calls {@link #finished()} on successful completion and {@link #finishedExceptionally(Throwable)}
 * on failure. Must also {@link #close()} this instance.
 */
public interface AddContents extends AutoCloseable {

  long addLiveContent(@NotNull Stream<ContentReference> contentReference);

  void finished();

  void finishedExceptionally(@NotNull Throwable e);

  Instant created();

  UUID id();

  /**
   * Closes this instance. Marks the identified set of contents as failed, if neither {@link
   * #finished()} nor {@link #finishedExceptionally(Throwable)} were called.
   */
  @Override
  void close();
}
