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
import java.util.Objects;
import java.util.function.Function;
import org.projectnessie.client.api.NessieApiV1;
import org.projectnessie.model.Reference;

final class GCBuilder implements GC.Builder {

  private NessieApiV1 api;
  private Instant defaultCutOffTimeStamp;
  private Function<Reference, Instant> cutOffTimeStampPerRefFunc;

  @Override
  public GC.Builder withApi(NessieApiV1 api) {
    this.api = api;
    return this;
  }

  @Override
  public GC.Builder withDefaultCutOffTimeStamp(Instant defaultCutOffTimeStamp) {
    this.defaultCutOffTimeStamp = defaultCutOffTimeStamp;
    return this;
  }

  @Override
  public GC.Builder withCutOffTimeStampPerReferenceFunc(
      Function<Reference, Instant> cutOffTimeStampPerReferenceFunc) {
    this.cutOffTimeStampPerRefFunc = cutOffTimeStampPerReferenceFunc;
    return this;
  }

  @Override
  public GC build() {
    Instant defaultCutOffTimeStamp =
        Objects.requireNonNull(
            this.defaultCutOffTimeStamp, "defaultCutOffTimeStamp must not be null");

    Function<Reference, Instant> cutOffTimestampFunc;
    if (cutOffTimeStampPerRefFunc == null) {
      cutOffTimestampFunc = ref -> defaultCutOffTimeStamp;
    } else {
      // map the missing reference's cutoff time to defaultCutOffTimeStamp.
      cutOffTimestampFunc =
          ref -> {
            Instant currentVal = cutOffTimeStampPerRefFunc.apply(ref);
            if (currentVal == null) {
              return defaultCutOffTimeStamp;
            } else {
              return currentVal;
            }
          };
    }

    return new GCImpl(api, cutOffTimestampFunc);
  }
}
