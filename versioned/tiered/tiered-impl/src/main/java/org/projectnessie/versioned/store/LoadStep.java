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
package org.projectnessie.versioned.store;

import java.util.Optional;
import java.util.stream.Collector;
import java.util.stream.Stream;

public interface LoadStep {

  Stream<LoadOp<?>> getOps();

  /**
   * Merge the current LoadStep with another to create a new compound LoadStep.
   * @param other The second LoadStep to combine with this.
   * @return A newly created combined LoadStep
   */
  LoadStep combine(LoadStep other);

  Optional<LoadStep> getNext();

  static Collector<LoadStep, ?, LoadStep> toLoadStep() {
    return LoadStepCollector.COLLECTOR;
  }

}
