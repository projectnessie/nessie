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
package org.projectnessie.nessie.tasks.api;

import java.util.concurrent.CompletionStage;
import org.projectnessie.versioned.storage.common.persist.Persist;

/**
 * Interface for task service implementation separating the stateful tasks service instance from the
 * stateless per-Nessie-repository {@link Tasks} instance.
 */
public interface TasksService {
  /** Retrieve the {@link Tasks} instance for a given {@link Persist}. */
  Tasks forPersist(Persist persist);

  CompletionStage<Void> shutdown();
}
