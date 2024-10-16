/*
 * Copyright (C) 2024 Dremio
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
package org.projectnessie.versioned.storage.cleanup;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import org.projectnessie.nessie.immutables.NessieImmutable;

@NessieImmutable
public interface PurgeStats {
  Instant started();

  Instant ended();

  default Duration duration() {
    return Duration.between(started(), ended());
  }

  /** Number of objects handled while scanning the Nessie repository. */
  long numScannedObjs();

  /**
   * Number of purged (deleted) objects. For a {@linkplain CleanupParams#dryRun() dry-run}, this
   * value indicates the number of objects that <em>would</em> have been deleted.
   */
  long numPurgedObjs();

  Optional<Exception> failure();
}
