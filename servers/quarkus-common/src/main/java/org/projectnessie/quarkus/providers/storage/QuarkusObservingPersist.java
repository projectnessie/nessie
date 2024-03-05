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
package org.projectnessie.quarkus.providers.storage;

import jakarta.inject.Singleton;
import org.projectnessie.quarkus.providers.NotObserved;
import org.projectnessie.quarkus.providers.UninitializedRepository;
import org.projectnessie.versioned.storage.common.persist.ObservingPersist;
import org.projectnessie.versioned.storage.common.persist.Persist;

/** CDI bean for {@link ObservingPersist}. */
@Singleton
@UninitializedRepository
public class QuarkusObservingPersist extends ObservingPersist {
  public QuarkusObservingPersist(@NotObserved Persist delegate) {
    super(delegate);
  }
}
