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
package org.projectnessie.gc.identify;

import java.time.Instant;
import javax.annotation.Nonnull;

@FunctionalInterface
public interface VisitedDeduplicator {
  VisitedDeduplicator NOOP = (cutOffTimestamp, commitId) -> false;

  boolean alreadyVisited(
      @Nonnull @jakarta.annotation.Nonnull Instant cutOffTimestamp,
      @Nonnull @jakarta.annotation.Nonnull String commitId);
}
