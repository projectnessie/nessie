/*
 * Copyright (C) 2020 Dremio
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
package org.projectnessie.versioned.persist.gc;

import java.time.Instant;
import java.util.function.Function;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.model.Reference;

public interface GC {
  static Builder builder() {
    return new GCBuilder();
  }

  <GC_CONTENT_VALUES extends ExpiredContentValues> GCResult<GC_CONTENT_VALUES> performGC(
      ContentValuesCollector<GC_CONTENT_VALUES> contentValuesCollector);

  interface Builder {
    Builder withApi(NessieApiV1 api);

    /**
     * Commits that are older than the specified {@code defaultCutOffTimeStamp} will be considered
     * as eligible for GC.
     *
     * <p>This is effectively a shortcut for {@link #withCutOffTimeStampPerReferenceFunc(Function)}.
     *
     * @see #withCutOffTimeStampPerReferenceFunc(Function)
     */
    Builder withDefaultCutOffTimeStamp(Instant defaultCutOffTimeStamp);

    /**
     * Adds a function to compute the timestamp after which commits for a named reference are
     * considered as expired.
     *
     * @see #withDefaultCutOffTimeStamp(Instant)
     */
    Builder withCutOffTimeStampPerReferenceFunc(
        Function<Reference, Instant> cutOffTimeStampPerReferenceFunc);

    /** Build the {@link GC} instance. */
    GC build();
  }
}
